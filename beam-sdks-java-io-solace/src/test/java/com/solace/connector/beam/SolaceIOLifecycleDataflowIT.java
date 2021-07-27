package com.solace.connector.beam;

import com.google.api.services.dataflow.model.Job;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.solace.connector.beam.test.fn.ExtractSolacePayloadFn;
import com.solace.connector.beam.test.transform.FileWriterPTransform;
import com.solace.connector.beam.test.util.GoogleDataflowUtil;
import com.solace.connector.beam.test.util.GoogleStorageUtil;
import com.solace.semp.v2.config.model.MsgVpnQueue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public class SolaceIOLifecycleDataflowIT extends ITBase {
	@Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
	private List<String> testQueues;

	private static final Logger LOG = LoggerFactory.getLogger(SolaceIOLifecycleDataflowIT.class);
	private static final String OUTPUT_FILE_PREFIX = "solace-IT-lifecycle-";

	@Before
	public void setupLifecycleTests() throws Exception {
		assumeTrue(String.format("Runner is %s instead of one of [%s, %s], skipping lifecycle tests",
				testPipeline.getOptions().getRunner(), DataflowRunner.class, TestDataflowRunner.class),
				testPipeline.getOptions().getRunner().equals(DataflowRunner.class) ||
				testPipeline.getOptions().getRunner().equals(TestDataflowRunner.class));

		testPipeline.getOptions().setRunner(DataflowRunner.class); // For async runs, but disables PAsserts

		testQueues = new ArrayList<>();
		provisionQueue(false);
		provisionQueue(true);
	}

	@After
	public void teardown() throws Exception {
		if (jcsmpSession != null && !jcsmpSession.isClosed() && testQueues != null) {
			for (String queueName : testQueues) {
				LOG.info(String.format("Deprovisioning Queue %s", queueName));
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
			}
		}
	}

	@Test
	public void testCreate() throws Exception {
		String outputGSUrlPrefix = generateOutputGSUrlPrefix();

		int numMsgsPerQueue = 40000;
		for (String queueName : testQueues) {
			populateQueue(queueName, numMsgsPerQueue);
		}

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper());

		testPipeline.apply(read)
				.apply(createIncrementalGlobalWindow(Duration.standardSeconds(5)))
				.apply(ParDo.of(new ExtractSolacePayloadFn()))
				.apply(new FileWriterPTransform(outputGSUrlPrefix));

		PipelineResult pipelineResult = null;
		try {
			pipelineResult = testPipeline.run();
			GoogleDataflowUtil.waitForJobToStart(pipelineOptions.as(DataflowPipelineOptions.class));
			sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, TimeUnit.MINUTES.toSeconds(5));
			verifyAllMessagesAreReceivedAndProcessed(outputGSUrlPrefix, numMsgsPerQueue);
		} finally {
			if (pipelineResult != null) {
				pipelineResult.cancel();
				pipelineResult.waitUntilFinish();
			}
		}
	}

	@Test
	public void testUpdate() throws Exception {
		String outputGSUrlPrefix = generateOutputGSUrlPrefix();
		int numMsgsPerQueue = 100000;
		long defaultMaxDeliveredUnackedMsgs = sempOps
				.getQueueMaxDeliveredUnackedMsgsPerFlow(testJcsmpProperties, testQueues.get(0));

		for (String queueName : testQueues) {
			long maxDeliveredUnackedMsgs = 100;
			LOG.info(String.format("Staggering message consumption for queue %s to %s max delivered unacked messages",
					queueName, maxDeliveredUnackedMsgs));
			sempOps.updateQueue(testJcsmpProperties, queueName,
					new MsgVpnQueue().maxDeliveredUnackedMsgsPerFlow(maxDeliveredUnackedMsgs));

			populateQueue(queueName, numMsgsPerQueue);
		}

		PipelineResult pipelineResult0 = null;
		PipelineResult pipelineResult1 = null;
		try {
			Pipeline initPipeline = Pipeline.create(pipelineOptions);

			initPipeline.apply("solace", SolaceIO.read(testJcsmpProperties, testQueues,
							SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper()))
					.apply("windowing", createIncrementalGlobalWindow(Duration.standardMinutes(5)))
					.apply("extract", ParDo.of(new ExtractSolacePayloadFn()))
					.apply("writeFile", new FileWriterPTransform(outputGSUrlPrefix));

			pipelineResult0 = initPipeline.run();
			GoogleDataflowUtil.waitForJobToStart(pipelineOptions.as(DataflowPipelineOptions.class));

			for (String queueName : testQueues) {
				long wait = TimeUnit.MINUTES.toMillis(5);
				long sleep = TimeUnit.SECONDS.toMillis(1);
				while (sempOps.getQueueMessageCount(testJcsmpProperties, queueName) >= numMsgsPerQueue) {
					if (wait > 0) {
						LOG.info(String.format("Waiting for queue %s to have less than %s messages - %s sec remaining",
								queueName, numMsgsPerQueue, TimeUnit.MILLISECONDS.toSeconds(wait)));
						Thread.sleep(sleep);
						wait -= sleep;
					} else {
						fail(String.format("Queue %s didn't consume enough messages", queueName));
					}
				}

				assertThat(String.format("Queue %s was emptied before job %s could be updated, " +
								"consider staggering message consumption rate", queueName, pipelineOptions.getJobName()),
						sempOps.getQueueMessageCount(testJcsmpProperties, queueName), greaterThan((long) 0));
			}

			Job jobToUpdate = GoogleDataflowUtil.getJob(pipelineOptions.as(DataflowPipelineOptions.class));

			LOG.info(String.format("Beginning to update pipeline job %s", pipelineOptions.getJobName()));
			pipelineOptions.as(DataflowPipelineOptions.class).setUpdate(true);

			testPipeline.apply("solace", SolaceIO.read(testJcsmpProperties, testQueues,
							SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper()))
					.apply("windowing", createIncrementalGlobalWindow(Duration.standardSeconds(5)))
					.apply("extract", ParDo.of(new ExtractSolacePayloadFn()))
					.apply("writeFile", new FileWriterPTransform(outputGSUrlPrefix));

			pipelineResult1 = testPipeline.run();
			GoogleDataflowUtil.waitForJobToUpdate(jobToUpdate);

			for (String queueName : testQueues) {
				assertThat(String.format("Queue %s was emptied before job %s could finish updating, " +
								"consider staggering message consumption rate", queueName, pipelineOptions.getJobName()),
						sempOps.getQueueMessageCount(testJcsmpProperties, queueName), greaterThan((long) 0));
				sempOps.updateQueue(testJcsmpProperties, queueName,
						new MsgVpnQueue().maxDeliveredUnackedMsgsPerFlow(defaultMaxDeliveredUnackedMsgs));
			}

			sempOps.waitForQueuesEmpty(testJcsmpProperties, testQueues, TimeUnit.MINUTES.toSeconds(5));
			verifyAllMessagesAreReceivedAndProcessed(outputGSUrlPrefix, numMsgsPerQueue);
		} finally {
			if (pipelineResult1 != null) {
				pipelineResult1.cancel();
				pipelineResult1.waitUntilFinish();
			} else if (pipelineResult0 != null) {
				pipelineResult0.cancel();
				pipelineResult0.waitUntilFinish();
			}
		}
	}

	@Test
	public void testCancel() throws Exception {
		for (String queueName : testQueues) {
			LOG.info(String.format("Restricting consumption rate for queue %s", queueName));
			sempOps.updateQueue(testJcsmpProperties, queueName,
					new MsgVpnQueue().maxDeliveredUnackedMsgsPerFlow((long) 10));
		}

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper());

		testPipeline.apply(read)
				.apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
				.apply(ParDo.of(new ExtractSolacePayloadFn()));

		long numMsgsPerQueue = 40000;

		PipelineResult pipelineResult = null;
		try {
			pipelineResult = testPipeline.run();
			GoogleDataflowUtil.waitForJobToStart(pipelineOptions.as(TestDataflowPipelineOptions.class));

			for (String queueName : testQueues) {
				populateQueue(queueName, numMsgsPerQueue);
			}

			for (String queueName : testQueues) {
				long wait = TimeUnit.MINUTES.toMillis(5);
				long sleep = TimeUnit.SECONDS.toMillis(1);
				while (sempOps.getQueueMessageCount(testJcsmpProperties, queueName) >= numMsgsPerQueue) {
					if (wait > 0) {
						LOG.info(String.format("Waiting for queue %s to have less than %s messages - %s sec remaining",
								queueName, numMsgsPerQueue, TimeUnit.MILLISECONDS.toSeconds(wait)));
						Thread.sleep(sleep);
						wait -= sleep;
					} else {
						fail(String.format("Queue %s didn't consume enough messages", queueName));
					}
				}

				assertThat(String.format("Queue %s was emptied before job %s could be cancelled, " +
								"consider staggering message consumption rate", queueName, pipelineOptions.getJobName()),
						sempOps.getQueueMessageCount(testJcsmpProperties, queueName), greaterThan((long) 0));
			}

			LOG.info(String.format("Cancelling job %s while messages are still in queue",
					testPipeline.getOptions().getJobName()));
		} finally {
			if (pipelineResult != null && !pipelineResult.getState().equals(PipelineResult.State.CANCELLED)) {
				pipelineResult.cancel();
				pipelineResult.waitUntilFinish();
			}
		}

		for (String queueName : testQueues) {
			assertThat(String.format("Expected 0 < msgs < %s on queue %s", numMsgsPerQueue, queueName),
					sempOps.getQueueMessageCount(testJcsmpProperties, queueName),
					allOf(greaterThan((long) 0), lessThan(numMsgsPerQueue)));
		}
	}

	@Test
	public void testDrain() throws Exception {
		String outputGSUrlPrefix = generateOutputGSUrlPrefix();

		for (String queueName : testQueues) {
			LOG.info(String.format("Restricting consumption rate for queue %s", queueName));
			sempOps.updateQueue(testJcsmpProperties, queueName,
					new MsgVpnQueue().maxDeliveredUnackedMsgsPerFlow((long) 10));
		}

		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper());

		testPipeline.apply(read)
				.apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
				.apply(ParDo.of(new ExtractSolacePayloadFn()))
				.apply(new FileWriterPTransform(outputGSUrlPrefix));

		long numMsgsPerQueue = 40000;

		PipelineResult pipelineResult = null;
		try {
			pipelineResult = testPipeline.run();
			GoogleDataflowUtil.waitForJobToStart(pipelineOptions.as(TestDataflowPipelineOptions.class));

			for (String queueName : testQueues) {
				populateQueue(queueName, numMsgsPerQueue);
			}

			for (String queueName : testQueues) {
				long wait = TimeUnit.MINUTES.toMillis(5);
				long sleep = TimeUnit.SECONDS.toMillis(1);
				while (sempOps.getQueueMessageCount(testJcsmpProperties, queueName) >= numMsgsPerQueue) {
					if (wait > 0) {
						LOG.info(String.format("Waiting for queue %s to have less than %s messages - %s sec remaining",
								queueName, numMsgsPerQueue, TimeUnit.MILLISECONDS.toSeconds(wait)));
						Thread.sleep(sleep);
						wait -= sleep;
					} else {
						fail(String.format("Queue %s didn't consume enough messages", queueName));
					}
				}

				assertThat(String.format("Queue %s was emptied before job %s could be drained, " +
								"consider staggering message consumption rate", queueName, pipelineOptions.getJobName()),
						sempOps.getQueueMessageCount(testJcsmpProperties, queueName), greaterThan((long) 0));
			}

			LOG.info(String.format("Draining job %s while messages are still in queue",
					testPipeline.getOptions().getJobName()));
			GoogleDataflowUtil.drainJob(pipelineOptions.as(TestDataflowPipelineOptions.class));
		} catch (Exception e) {
			if (pipelineResult != null && !pipelineResult.getState().equals(PipelineResult.State.CANCELLED)) {
				pipelineResult.cancel();
				pipelineResult.waitUntilFinish();
			}

			throw e;
		}

		long totalNumUnsentMsgs = 0;

		for (String queueName : testQueues) {
			long numMsgsOnQueue = sempOps.getQueueMessageCount(testJcsmpProperties, queueName);
			assertThat(String.format("Expected 0 < msgs < %s on queue %s", numMsgsPerQueue, queueName), numMsgsOnQueue,
					allOf(greaterThan((long) 0), lessThan(numMsgsPerQueue)));
			assertThat(String.format("Unacked messages were found on queue %s", queueName),
					sempOps.getQueueUnackedMessageCount(testJcsmpProperties, queueName), lessThanOrEqualTo((long) 0));

			totalNumUnsentMsgs += numMsgsOnQueue;
		}

		assertEquals(numMsgsPerQueue * getNumUniqueQueues(),
				totalNumUnsentMsgs + getNumReceivedMsgs(outputGSUrlPrefix));
	}

	private void provisionQueue(boolean isExclusive) throws JCSMPException {
		String queueName = UUID.randomUUID().toString();
		testQueues.add(queueName);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType(isExclusive ? EndpointProperties.ACCESSTYPE_EXCLUSIVE :
				EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

		LOG.info(String.format("Provisioning %s queue %s", isExclusive ? "exclusive" : "non-exclusive", queueName));
		jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);
	}

	private int getNumUniqueQueues() {
		return new HashSet<>(testQueues).size();
	}

	private void populateQueue(String queueName, long numMsgs) throws JCSMPException, InterruptedException {
		Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		AtomicLong numDelivered = new AtomicLong(0);
		AtomicReference<JCSMPException> sendException = new AtomicReference<>();
		LOG.info(String.format("Publishing %s messages to queue %s", numMsgs, queue.getName()));
		for (int i = 0; i < numMsgs; i++) {
			String payload = String.format("%s - %s", queueName, i);
			BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
			msg.writeAttachment(payload.getBytes(StandardCharsets.UTF_8));
			msg.setDeliveryMode(DeliveryMode.PERSISTENT);
			CallbackCorrelationKey correlationKey = new CallbackCorrelationKey();
			correlationKey.setOnSuccess(numDelivered::incrementAndGet);
			correlationKey.setOnFailure((e, timeout) -> sendException.set(e));
			msg.setCorrelationKey(correlationKey);
			producer.send(msg, queue);
		}

		final long deliveryWaitExpiry = TimeUnit.MINUTES.toMillis(5) + System.currentTimeMillis();
		while (numDelivered.get() < numMsgs && sendException.get() == null) {
			long realTimeout = Math.min(deliveryWaitExpiry - System.currentTimeMillis(), 5000);
			if (realTimeout <= 0) {
				fail(String.format("Timed out while waiting for %s messages to be delivered to queue %s, only got %s",
						numMsgs, queueName, numDelivered.get()));
			}
			LOG.info(String.format("%s of %s messages sent to queue %s", numDelivered.get(), numMsgs, queueName));
			Thread.sleep(realTimeout);
		}
		assertNull(String.format("Failed to send message to queue %s", queueName), sendException.get());
		assertEquals(String.format("All messages weren't published to queue %s", queueName), numMsgs,
				numDelivered.get());
		LOG.info(String.format("%s messages successfully sent to queue %s", numMsgs, queueName));
	}

	private void verifyAllMessagesAreReceivedAndProcessed(String outputGSUrlPrefix, int numMsgsPerQueue)
			throws IOException, InterruptedException {
		long wait = TimeUnit.MINUTES.toMillis(10);
		long sleep = Math.min(TimeUnit.SECONDS.toMillis(5), wait);
		int totalExpectedMsgs = numMsgsPerQueue * getNumUniqueQueues();
		int numReceivedMsgs;
		while ((numReceivedMsgs = getNumReceivedMsgs(outputGSUrlPrefix)) < totalExpectedMsgs && wait > 0) {
			LOG.info(String.format("Waiting for %s messages in Google Storage objects %s - %s sec remaining",
					totalExpectedMsgs, outputGSUrlPrefix, TimeUnit.MILLISECONDS.toSeconds(wait)));
			Thread.sleep(sleep);
			wait -= sleep;
		}

		assertEquals(String.format("Incorrect # of messages found in Google Storage object %s", outputGSUrlPrefix),
				totalExpectedMsgs, numReceivedMsgs);
	}

	private int getNumReceivedMsgs(String gsUrlPrefix) throws IOException {
		BlobId outputBlobPrefix = GoogleStorageUtil.createBlobId(gsUrlPrefix);
		Set<String> receivedMsgs = new HashSet<>();

		LOG.info(String.format("Downloading all Google Cloud Storage objects in bucket %s with prefix %s",
				outputBlobPrefix.getBucket(), outputBlobPrefix.getName()));

		for (Blob outputBlob : GoogleStorageUtil.getBlobs(
				pipelineOptions.as(DataflowPipelineOptions.class).getProject(),
				outputBlobPrefix.getBucket(), outputBlobPrefix.getName())) {
			File downloadedBlob = tmpFolder.newFile();
			outputBlob.downloadTo(downloadedBlob.toPath());

			try (Scanner scanner = new Scanner(downloadedBlob)) {
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					if (receivedMsgs.contains(line)) {
						LOG.info(String.format("Received duplicate message %s", line));
					} else {
						receivedMsgs.add(line);
					}
				}
			}
		}

		return receivedMsgs.size();
	}

	/**
	 * <p> Creates a new global window {@link PTransform} that processes whatever data it receives
	 * over a period of the specified duration.
	 * <p> Note that this is different than using {@link FixedWindows} which partitions its data based off their
	 * timestamps.
	 * @param <T> The type of messages
	 * @return a new Window {@link PTransform}
	 * @see <a href="https://stackoverflow.com/a/44032176" target="_top">More indepth explanation</a>
	 */
	private <T> Window<T> createIncrementalGlobalWindow(Duration duration) {
		return Window.<T>into(new GlobalWindows())
				.triggering(Repeatedly
						.forever(AfterProcessingTime
								.pastFirstElementInPane()
								.plusDelayOf(duration)
						)
				)
				.withAllowedLateness(Duration.ZERO)
				.discardingFiredPanes();
	}

	private String generateOutputGSUrlPrefix() {
		return String.join("/",
				pipelineOptions.as(TestPipelineOptions.class).getTempRoot(),
				"output",
				pipelineOptions.getJobName(),
				OUTPUT_FILE_PREFIX);
	}
}
