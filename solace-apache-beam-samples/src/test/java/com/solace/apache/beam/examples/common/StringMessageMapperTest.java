package com.solace.apache.beam.examples.common;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class StringMessageMapperTest {
	StringMessageMapper mapper;

	@Before
	public void setup() {
		mapper = new StringMessageMapper();
	}

	@Test
	public void testMapText() {
		String expectedPayload = "test Me123 !@";
		TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setText(expectedPayload);
		assertEquals(expectedPayload, mapper.map(msg));
	}

	@Test
	public void testMapBytes() {
		String expectedPayload = "test Me123 !@";
		BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		msg.setData(expectedPayload.getBytes(StandardCharsets.UTF_8));
		assertEquals(expectedPayload, mapper.map(msg));
	}

	@Test
	public void testMapMap() throws Exception {
		SDTMap expectedPayload = JCSMPFactory.onlyInstance().createMap();
		expectedPayload.putString("test", "test Me123 !@");
		MapMessage msg = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
		msg.setMap(expectedPayload);
		assertEquals(expectedPayload.toString(), mapper.map(msg));
	}

	@Test
	public void testMapStream() {
		SDTStream expectedPayload = JCSMPFactory.onlyInstance().createStream();
		expectedPayload.writeString("test Me123 !@");
		StreamMessage msg = JCSMPFactory.onlyInstance().createMessage(StreamMessage.class);
		msg.setStream(expectedPayload);
		assertEquals(expectedPayload.toString(), mapper.map(msg));
	}

	@Test
	public void testMapXml() {
		String expectedPayload = "test Me123 !@";
		BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
		msg.writeBytes(expectedPayload.getBytes());
		msg.setReadOnly();
		assertEquals(expectedPayload, mapper.map(msg).trim());
	}

	@Test
	public void testMapEmpty() {
		BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
		msg.setReadOnly();
		assertEquals("", mapper.map(msg).trim());
	}
}
