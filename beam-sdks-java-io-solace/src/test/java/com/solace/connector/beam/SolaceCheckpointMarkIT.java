package com.solace.connector.beam;

import com.solace.connector.beam.test.extension.BeamPubSubPlusExtension;
import com.solace.connector.beam.test.pubsub.PublisherEventHandler;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.joda.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ExtendWith(BeamPubSubPlusExtension.class)
public class SolaceCheckpointMarkIT {
	private FlowReceiver flowReceiver;
	private UnboundedSolaceReader<SolaceTestRecord> reader;
	private BlockingQueue<UnboundedSolaceReader.Message> ackQueue;
	private Instant firstWatermark;
	private Instant lastWatermark;

	private static final Logger LOG = LoggerFactory.getLogger(SolaceCheckpointMarkIT.class);

	@BeforeEach
	public void setup(JCSMPSession jcsmpSession, JCSMPProperties jcsmpProperties, Queue queue) throws Exception {
		reader = new UnboundedSolaceReader<>(new UnboundedSolaceSource<>(SolaceIO.read(
				jcsmpProperties,
				Collections.singletonList(queue.getName()),
				SolaceTestRecord.getCoder(),
				SolaceTestRecord.getMapper())));
		reader.active.set(true); //Fake it don't start it!
		firstWatermark = new Instant(reader.watermark.get());

		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		consumerFlowProperties.setEndpoint(queue);
		consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

		flowReceiver = jcsmpSession.createFlow(null, consumerFlowProperties);
		flowReceiver.start();

		ackQueue = new LinkedBlockingQueue<>();
		XMLMessageProducer producer = jcsmpSession.getMessageProducer(new PublisherEventHandler());
		try {
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
		} finally {
			producer.close();
		}
	}

	@AfterEach
	public void teardown() throws Exception {
		if (reader != null) {
			reader.close();
		}

		if (flowReceiver != null) {
			flowReceiver.close();
		}
	}

	@Test
	public void testFinalize(JCSMPSession jcsmpSession, SempV2Api sempV2Api, Queue queue) throws Exception {
		SolaceCheckpointMark checkpointMark = new SolaceCheckpointMark(reader, (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME), ackQueue);
		checkpointMark.finalizeCheckpoint();
		assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
			while (sempV2Api.monitor()
					.getMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queue.getName(), null)
					.getData()
					.getMsgSpoolUsage() <= 0) {
				Thread.sleep(1000);
			}
		});
		assertEquals(lastWatermark, reader.getWatermark());
	}

	@Test
	public void testEmptyQueue(JCSMPSession jcsmpSession, SempV2Api sempV2Api, Queue queue) throws Exception {
		ackQueue = new LinkedBlockingQueue<>();
		SolaceCheckpointMark checkpointMark = new SolaceCheckpointMark(reader, (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME), ackQueue);
		checkpointMark.finalizeCheckpoint();
		Thread.sleep(5000);
		assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queue.getName(), null)
				.getData()
				.getMsgSpoolUsage() <= 0);
		assertEquals(firstWatermark, reader.getWatermark());
	}

	@Test
	public void testInactiveReader(JCSMPSession jcsmpSession, SempV2Api sempV2Api, Queue queue) throws Exception {
		reader.active.set(false);
		SolaceCheckpointMark checkpointMark = new SolaceCheckpointMark(reader, (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME), ackQueue);
		checkpointMark.finalizeCheckpoint();
		Thread.sleep(5000);
		assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queue.getName(), null)
				.getData()
				.getMsgSpoolUsage() <= 0);
		assertEquals(firstWatermark, reader.getWatermark());
	}
}
