package com.solace.connector.beam;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class SolaceCheckpointMarkIT extends ITBase {
	private FlowReceiver flowReceiver;
	private String clientName;
	private UnboundedSolaceReader<SolaceTestRecord> reader;
	private Queue queue;
	private BlockingQueue<UnboundedSolaceReader.Message> ackQueue;
	private Instant firstWatermark;
	private Instant lastWatermark;

	private static final Logger LOG = LoggerFactory.getLogger(SolaceCheckpointMarkIT.class);

	@Before
	public void setup() throws Exception {
		clientName = (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME);

		queue = JCSMPFactory.onlyInstance().createQueue(UUID.randomUUID().toString());
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

		LOG.info(String.format("Provisioning Queue %s", queue.getName()));
		jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

		reader = new UnboundedSolaceReader<>(new UnboundedSolaceSource<>(SolaceIO.read(
				testJcsmpProperties,
				Collections.singletonList(queue.getName()),
				SolaceTestRecord.getCoder(),
				SolaceTestRecord.getMapper())));
		reader.active.set(true); //Fake it don't start it!
		firstWatermark = new Instant(reader.watermark.get());

		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		consumerFlowProperties.setEndpoint(queue);
		consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

		flowReceiver = jcsmpSession.createFlow(null, consumerFlowProperties, endpointProperties);
		flowReceiver.start();

		ackQueue = new LinkedBlockingQueue<>();
		for (long i = 0; i < 3; i++) {
			TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			msg.setText(String.format("%s - %s", queue.getName(), i));
			LOG.info(String.format("Sending to queue %s a message with payload %s", queue.getName(), msg.getText()));
			producer.send(msg, queue);

			BytesXMLMessage receivedMsg = flowReceiver.receive(5000);
			assertNotNull(receivedMsg);

			lastWatermark = new Instant(System.currentTimeMillis() + i);
			ackQueue.add(new UnboundedSolaceReader.Message(receivedMsg, lastWatermark));
		}
	}

	@After
	public void teardown() throws Exception {
		if (jcsmpSession != null && !jcsmpSession.isClosed()) {
			if (flowReceiver != null) {
				flowReceiver.close();
			}

			LOG.info(String.format("Deprovisioning Queue %s", queue.getName()));
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testFinalize() throws Exception {
		SolaceCheckpointMark checkpointMark = new SolaceCheckpointMark(reader, clientName, ackQueue);
		checkpointMark.finalizeCheckpoint();
		sempOps.waitForQueuesEmpty(testJcsmpProperties, Collections.singleton(queue.getName()), 30);
		assertEquals(lastWatermark, reader.getWatermark());
	}

	@Test
	public void testEmptyQueue() throws Exception {
		ackQueue = new LinkedBlockingQueue<>();
		SolaceCheckpointMark checkpointMark = new SolaceCheckpointMark(reader, clientName, ackQueue);
		checkpointMark.finalizeCheckpoint();
		Thread.sleep(5000);
		assertFalse(sempOps.isQueueEmpty(testJcsmpProperties, queue.getName()));
		assertEquals(firstWatermark, reader.getWatermark());
	}

	@Test
	public void testInactiveReader() throws Exception {
		reader.active.set(false);
		SolaceCheckpointMark checkpointMark = new SolaceCheckpointMark(reader, clientName, ackQueue);
		checkpointMark.finalizeCheckpoint();
		Thread.sleep(5000);
		assertFalse(sempOps.isQueueEmpty(testJcsmpProperties, queue.getName()));
		assertEquals(firstWatermark, reader.getWatermark());
	}
}
