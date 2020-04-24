package com.solace.connector.beam.examples.common;

import com.solace.connector.beam.SolaceIO;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;

import java.nio.charset.StandardCharsets;

public class StringMessageMapper implements SolaceIO.InboundMessageMapper<String> {
	private static final long serialVersionUID = 42L;

	@Override
	public String map(BytesXMLMessage message) {
		if (message instanceof TextMessage) {
			return ((TextMessage) message).getText();
		} else if (message instanceof BytesMessage) {
			return new String(((BytesMessage) message).getData(), StandardCharsets.UTF_8);
		} else if (message instanceof StreamMessage) {
			return ((StreamMessage) message).getStream().toString();
		} else if (message instanceof MapMessage) {
			return ((MapMessage) message).getMap().toString();
		} else if (message.getAttachmentContentLength() > 0) {
			return new String(message.getAttachmentByteBuffer().array(), StandardCharsets.UTF_8);
		} else {
			return new String(message.getBytes(), StandardCharsets.UTF_8);
		}
	}
}
