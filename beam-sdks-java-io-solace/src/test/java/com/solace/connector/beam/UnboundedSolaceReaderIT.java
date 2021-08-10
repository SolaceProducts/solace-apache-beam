package com.solace.connector.beam;

import com.solace.connector.beam.test.extension.BeamPubSubPlusExtension;
import com.solace.connector.beam.test.pubsub.PublisherEventHandler;
import com.solace.connector.beam.test.util.ThrowingConsumer;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(BeamPubSubPlusExtension.class)
public class UnboundedSolaceReaderIT {
	private UnboundedSolaceReader<SolaceTestRecord> reader;

	@BeforeEach
	void setUp(JCSMPSession jcsmpSession, JCSMPProperties jcsmpProperties, Queue queue) throws Exception {
		reader = new UnboundedSolaceReader<>(new UnboundedSolaceSource<>(SolaceIO.read(
				jcsmpProperties,
				Collections.singletonList(queue.getName()),
				SolaceTestRecord.getCoder(),
				SolaceTestRecord.getMapper()))
				.split(1, null)
				.get(0));

		XMLMessageProducer producer = jcsmpSession.getMessageProducer(new PublisherEventHandler());
		try {
			IntStream.range(0, 10)
					.mapToObj(i -> JCSMPFactory.onlyInstance().createMessage(BytesMessage.class))
					.peek(m -> m.setData(RandomUtils.nextBytes(100)))
					.forEach((ThrowingConsumer<XMLMessage>) m -> producer.send(m, queue));
		} finally {
			producer.close();
		}
	}

	@AfterEach
	void tearDown() throws Exception {
		if (reader != null) {
			reader.close();
		}
	}

	@Test
	public void testGetSplitBacklogBytesWithoutStarting(JCSMPProperties jcsmpProperties, SempV2Api sempV2Api, Queue queue) throws Exception {
		Long expectedSpooledByteCount = sempV2Api.monitor()
				.getMsgVpnQueue(jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME), queue.getName(), null)
				.getData()
				.getSpooledByteCount();
		assertEquals(expectedSpooledByteCount, reader.getSplitBacklogBytes());
	}

	@Test
	public void testConsecutiveSetup() throws Exception {
		reader.setUp();
		String clientName1 = reader.getClientName();
		reader.setUp();
		String clientName2 = reader.getClientName();
		assertEquals(clientName1, clientName2);
	}
}
