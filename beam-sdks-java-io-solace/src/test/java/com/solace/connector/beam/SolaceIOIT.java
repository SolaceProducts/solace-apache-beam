package com.solace.connector.beam;

import com.solace.connector.beam.test.fn.ExtractSolacePayloadFn;
import com.solace.connector.beam.test.transform.CountMessagesPTransform;
import com.solace.connector.beam.test.util.GoogleDataflowUtil;
import com.solace.semp.v2.action.ApiException;
import com.solace.semp.v2.config.model.MsgVpnQueue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class SolaceIOIT extends ITBase {
	@Rule public ExpectedException thrown = ExpectedException.none();

	private List<String> testQueues;
	private List<String> expectedMsgPayloads;
	private EndpointProperties endpointProperties;

	private static final Logger LOG = LoggerFactory.getLogger(SolaceIOIT.class);
	private static final int NUM_MSGS_PER_QUEUE = 10;
	private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

	@AfterClass
	public static void globalTeardown() {
		if (SCHEDULER.isTerminated()) {
			SCHEDULER.shutdownNow();
		}
	}

	@Before
	public void setup() throws Exception {
		testQueues = new ArrayList<>();
		expectedMsgPayloads = new ArrayList<>();
		provisionQueue();
	}

	@After
	public void teardown() throws Exception {
		if (jcsmpSession != null && !jcsmpSession.isClosed()) {
			for (String queueName : testQueues) {
				LOG.info(String.format("Deprovisioning Queue %s", queueName));
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
			}
		}
	}

	@Test
	public void testBasic() throws Exception {
		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxNumRecords(NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testMultiQueue() throws Exception {
		while (testQueues.size() < 2) provisionQueue();

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxNumRecords(NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testMultiOnSameQueue() throws Exception {
		testQueues = Arrays.asList(testQueues.get(0), testQueues.get(0));

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxNumRecords(NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testExclusiveQueue() throws Exception {
		sempOps.updateQueue(testJcsmpProperties, testQueues.get(0),
				new MsgVpnQueue().accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE));

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxNumRecords(NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testMultiOnSameExclusiveQueue() throws Exception {
		testQueues = Arrays.asList(testQueues.get(0), testQueues.get(0));
		sempOps.updateQueue(testJcsmpProperties, testQueues.get(0),
				new MsgVpnQueue().accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE));

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxNumRecords(NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testSendDuplicateMessage() throws Exception {
		drainQueues();
		Queue queue = JCSMPFactory.onlyInstance().createQueue(testQueues.get(0));

		LOG.info(String.format("Sending %s duplicate message to queue %s", NUM_MSGS_PER_QUEUE, queue.getName()));
		for (int i = 0; i < NUM_MSGS_PER_QUEUE; i++) {
			BytesXMLMessage msg = createMessage(queue.getName(), "test");
			producer.send(msg, queue);
		}

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxNumRecords(NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testFailBrokerCxn() {
		testJcsmpProperties.setProperty(JCSMPProperties.HOST, "localhost");
		testJcsmpProperties.setProperty(JCSMPProperties.USERNAME, "dummy");
		testJcsmpProperties.setProperty(JCSMPProperties.PASSWORD, "dummy");
		testJcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, "dummy");

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper());

		thrown.expect(RuntimeException.class);

		// For whatever reason Google Dataflow isn't returning nested Exceptions
		// Restricting these assertions to non Dataflow runners
		if (!pipelineOptions.getRunner().equals(TestDataflowRunner.class)) {
			thrown.expectCause(instanceOf(IOException.class));
			thrown.expectMessage("Failed to start UnboundSolaceReader");
			thrown.expectCause(hasProperty("cause", allOf(
					instanceOf(JCSMPTransportException.class),
					hasProperty("message", containsString("Error communicating with the router"))
			)));
		}

		testPipeline.apply(read);
		testPipeline.run();
	}

	@Test
	public void testQueueNotFound() {
		testQueues.add(UUID.randomUUID().toString());

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper());

		thrown.expect(RuntimeException.class);

		// For whatever reason Google Dataflow isn't returning nested Exceptions
		// Restricting these assertions to only DirectRunner
		if (!pipelineOptions.getRunner().equals(TestDataflowRunner.class)) {
			thrown.expectCause(instanceOf(IOException.class));
			thrown.expectMessage("Failed to start UnboundSolaceReader");
			thrown.expectCause(hasProperty("cause", allOf(
					instanceOf(JCSMPErrorResponseException.class),
					hasProperty("message", containsString("Unknown Queue"))
			)));
		}

		testPipeline.apply(read);
		testPipeline.run();
	}

	@Test
	public void testMultiPublisher() throws Exception {
		while (testQueues.size() < 2) provisionQueue();
		drainQueues();

		for (String queueName : testQueues) {
			Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			List<JCSMPSession> jcsmpSessions = new ArrayList<>();
			List<XMLMessageProducer> producers = new ArrayList<>();
			try {
				for (int i = 0; i < NUM_MSGS_PER_QUEUE; i++) {
					LOG.info(String.format("Creating JCSMP Session #%s for %s", i, testJcsmpProperties.getStringProperty(JCSMPProperties.HOST)));
					JCSMPSession jcsmpSession = JCSMPFactory.onlyInstance().createSession(testJcsmpProperties);
					jcsmpSessions.add(jcsmpSession);

					LOG.info(String.format("Creating XMLMessageProducer for %s", testJcsmpProperties.getStringProperty(JCSMPProperties.HOST)));
					XMLMessageProducer producer = jcsmpSession.getMessageProducer(new PublisherEventHandler());
					producers.add(producer);

					LOG.info(String.format("Sending message %s to queue %s", i, queueName));
					producer.send(createMessage(queueName, String.valueOf(i)), queue);
				}
			} finally {
				producers.forEach(XMLMessageProducer::close);
				jcsmpSessions.forEach(JCSMPSession::closeSession);
			}
		}

		LOG.info("Creating pipeline...");
		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxNumRecords(NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) NUM_MSGS_PER_QUEUE * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testSessionReconnect() throws Exception {
		while (testQueues.size() < 2) provisionQueue();
		drainQueues();

		int numMsgsPerQueue = 2;
		Map<String, BytesXMLMessage> nexMessageToAdd = new HashMap<>();

		for (String queueName : testQueues) {
			LOG.info(String.format("Sending new message to queue %s", queueName));
			BytesXMLMessage msg = createMessage(queueName, "1");
			producer.send(msg, JCSMPFactory.onlyInstance().createQueue(queueName));

			String msgPayload = SolaceTestRecord.getMapper().map(msg).getPayload();
			LOG.info(String.format("Adding duplicate message %s to expectedMsgPayloads", msgPayload));
			expectedMsgPayloads.add(msgPayload);

			nexMessageToAdd.put(queueName, createMessage(queueName, "2"));
		}

		SCHEDULER.schedule(() -> {
			waitForPipelineToStart();

			for (String queueName : testQueues) {
				waitForAllMessagesToBeReceived(queueName);
			}

			try {
				sempOps.disconnectClients(testJcsmpProperties,
						Collections.singleton((String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME)));
				Thread.sleep(5000);
			} catch (Exception e) {
				LOG.error("Failed to disconnect all clients", e);
				throw new RuntimeException(e);
			}

			for (String queueName : testQueues) {
				LOG.info(String.format("Sending new message to queue %s", queueName));
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				try {
					producer.send(nexMessageToAdd.get(queueName), queue);
				} catch (JCSMPException e) {
					LOG.error(String.format("Failed to send message to queue %s", queueName), e);
					throw new RuntimeException(e);
				}
			}
		}, 15, TimeUnit.SECONDS);

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxReadTime(Duration.standardMinutes(3)) // Timeout in case something breaks in a thread

				// Inflight messages will be re-sent due to reconnect
				.withMaxNumRecords((2 * numMsgsPerQueue - 1) * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) (2 * numMsgsPerQueue - 1) * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	@Test
	public void testFlowReconnect() throws Exception {
		while (testQueues.size() < 2) provisionQueue();

		LOG.info("Adding duplicates of expectedMsgPayloads since we're expecting some message duplication");
		expectedMsgPayloads.addAll(new ArrayList<>(expectedMsgPayloads));

		Map<String, BytesXMLMessage> messagesToResendPerQueue = new HashMap<>();
		for (String queueName : testQueues) {
			// Need to pre-consume a message to prevent the batched/blocking pipeline from closing
			LOG.info(String.format("Pre-consuming one message from queue %s", queueName));
			SolaceTestRecord message = removeOneMessageFromQueue(queueName);
			expectedMsgPayloads.remove(message.getPayload()); // Removing the duplicate
			messagesToResendPerQueue.put(queueName, createMessage(queueName, message.getPayload()));
		}

		SCHEDULER.schedule(() -> {
			waitForPipelineToStart();

			for (String queueName : testQueues) {
				waitForAllMessagesToBeReceived(queueName);

				try {
					sempOps.shutdownQueueEgress(testJcsmpProperties, queueName);
					Thread.sleep(5000);
				} catch (Exception e) {
					LOG.error(String.format("Failed to shutdown queue egress for queue %s", queueName), e);
					throw new RuntimeException(e);
				}

				LOG.info(String.format("Sending new message to queue %s", queueName));
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				try {
					producer.send(messagesToResendPerQueue.get(queueName), queue);
				} catch (JCSMPException e) {
					LOG.error(String.format("Failed to send message to queue %s", queueName), e);
					throw new RuntimeException(e);
				}
			}

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LOG.error("Sleep interrupted", e);
				throw new RuntimeException(e);
			}

			for (String queueName : testQueues) {
				try {
					sempOps.enableQueueEgress(testJcsmpProperties, queueName);
				} catch (Exception e) {
					LOG.error(String.format("Failed to enable queue egress for queue %s", queueName), e);
					throw new RuntimeException(e);
				}
			}
		}, 15, TimeUnit.SECONDS);

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
				.withMaxReadTime(Duration.standardMinutes(3)) // Timeout in case something breaks in a thread

				// Inflight messages will be re-sent due to reconnect
				.withMaxNumRecords((2 * NUM_MSGS_PER_QUEUE - 1) * getNumUniqueQueues());

		PCollection<String> messagePayloads = testPipeline.apply(read).apply(ParDo.of(new ExtractSolacePayloadFn()));
		PCollection<Long> counts = messagePayloads.apply(new CountMessagesPTransform());

		PAssert.that(messagePayloads).containsInAnyOrder(expectedMsgPayloads);
		PAssert.that(counts).containsInAnyOrder((long) (2 * NUM_MSGS_PER_QUEUE - 1) * getNumUniqueQueues());

		testPipeline.run();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, 30);
	}

	private void provisionQueue() throws JCSMPException {
		String queueName = UUID.randomUUID().toString();
		testQueues.add(queueName);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

		LOG.info(String.format("Provisioning Queue %s", queueName));
		jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

		for (int i = 0; i < NUM_MSGS_PER_QUEUE; i++) {
			producer.send(createMessage(queueName, String.valueOf(i)), queue);
		}
	}

	private void drainQueues() throws ApiException {
		sempOps.drainQueues(testJcsmpProperties, testQueues);
		expectedMsgPayloads = new ArrayList<>();
	}

	private int getNumUniqueQueues() {
		return new HashSet<>(testQueues).size();
	}

	private SolaceTestRecord removeOneMessageFromQueue(String queueName) throws Exception {
		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		consumerFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(queueName));
		FlowReceiver flowReceiver = jcsmpSession.createFlow(null, consumerFlowProperties, endpointProperties);
		flowReceiver.start();
		BytesXMLMessage wireMessage = flowReceiver.receive(5000);
		assertNotNull(wireMessage);
		wireMessage.ackMessage();
		flowReceiver.close();

		SolaceTestRecord message = SolaceTestRecord.getMapper().map(wireMessage);
		expectedMsgPayloads.remove(message.getPayload());
		LOG.info(String.format("Removed message from queue %s:\n%s", queueName, message));
		return message;
	}

	/**
	 * Must not be called from within a thread.
	 * @param queueName Name of the queue.
	 * @param modifier Modifier to differentiate between messages on a queue.
	 * @return new message
	 */
	private BytesXMLMessage createMessage(String queueName, String modifier) {
		String payload = String.format("%s - %s", queueName, modifier);
		LOG.info(String.format("Creating message with payload \"%s\"", payload));
		expectedMsgPayloads.add(payload);
		BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
		msg.writeAttachment(payload.getBytes(StandardCharsets.UTF_8));
		return msg;
	}

	private void waitForPipelineToStart() {
		if (pipelineOptions.getRunner().equals(TestDataflowRunner.class)) {
			TestDataflowPipelineOptions dataflowOptions = pipelineOptions.as(TestDataflowPipelineOptions.class);
			try {
				GoogleDataflowUtil.waitForJobToStart(dataflowOptions);
			} catch (Exception e) {
				LOG.error(String.format("Failed to retrieve Google Dataflow job %s", dataflowOptions.getJobName()), e);
				throw new RuntimeException(e);
			}
		}
	}

	private void waitForAllMessagesToBeReceived(String queueName) {
		try {
			long retryInterval = TimeUnit.SECONDS.toMillis(5);
			long maxAttempts = TimeUnit.MINUTES.toMillis(5) / retryInterval;
			boolean debounce = false;

			for (int attempts = 0; attempts < maxAttempts; attempts++) {
				long unackedMsgCount = sempOps.getQueueUnackedMessageCount(testJcsmpProperties, queueName);
				long msgCount = sempOps.getQueueMessageCount(testJcsmpProperties, queueName);

				if (unackedMsgCount >= msgCount) {
					if (debounce) {
						break;
					} else {
						debounce = true;
					}
				} else {
					LOG.info(String.format("Waiting for %s of %s enqueued messages to be sent from queue %s",
							msgCount - unackedMsgCount, msgCount, queueName));

					debounce = false;
					Thread.sleep(retryInterval);

					if (attempts >= maxAttempts - 1) {
						String msg = String.format("Timed out while waiting for all messages to be sent from queue %s",
								queueName);
						LOG.error(msg);
						throw new TimeoutException(msg);
					}
				}
			}
		} catch (Exception e) {
			LOG.error(String.format("Failed to get stats from queue %s", queueName), e);
			throw new RuntimeException(e);
		}
	}
}
