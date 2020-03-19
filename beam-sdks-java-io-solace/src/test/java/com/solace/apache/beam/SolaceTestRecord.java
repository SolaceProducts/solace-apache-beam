package com.solace.apache.beam;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class SolaceTestRecord implements Serializable {
	private static final long serialVersionUID = 42L;

	// Application properties
	private final Map<String, Object> properties;
	private final String text;

	private final String destination;
	private final long expiration;
	private final long messageId;
	private final int priority;
	private final boolean redelivered;
	private final String replyTo;
	private final long receiveTimestamp;
	private final long senderTimestamp;
	private final String senderId;
	private final long timeToLive;
	private final long sequenceNumber;

	/**
	 * Define a new Solace text record.
	 */
	public SolaceTestRecord(Map<String, Object> properties, String text, String destination, long expiration, long messageId, int priority, boolean redelivered, String replyTo, long receiveTimestamp, long senderTimestamp, String senderId, long timeToLive, long sequenceNumber) {
		this.properties = properties;
		this.text = text;
		this.destination = destination;
		this.expiration = expiration;
		this.messageId = messageId;
		this.priority = priority;
		this.redelivered = redelivered;
		this.replyTo = replyTo;
		this.receiveTimestamp = receiveTimestamp;
		this.senderTimestamp = senderTimestamp;
		this.senderId = senderId;
		this.timeToLive = timeToLive;
		this.sequenceNumber = sequenceNumber;
	}

	/**
	 * Return the text record destination.
	 */
	public String getDestination() {
		return destination;
	}

	/**
	 * Return the text record expiration.
	 */
	public long getExpiration() {
		return expiration;
	}

	/**
	 * Return the text record messageId.
	 */
	public long getMessageId() {
		return messageId;
	}

	/**
	 * Return the text record priority.
	 */
	public int getPriority() {
		return priority;
	}

	/**
	 * Return the text record properties.
	 */
	public Map<String, Object> getProperties() {
		return properties;
	}

	/**
	 * Return the text record redelivered.
	 */
	public boolean isRedelivered() {
		return redelivered;
	}

	/**
	 * Return the text record receiveTimestamp.
	 */
	public long getReceiveTimestamp() {
		return receiveTimestamp;
	}

	/**
	 * Return the text record replyTo.
	 */
	public String getReplyTo() {
		return replyTo;
	}

	/**
	 * Return the text record senderId.
	 */
	public String getSenderId() {
		return senderId;
	}

	/**
	 * Return the text record senderTimestamp.
	 */
	public long getSenderTimestamp() {
		return senderTimestamp;
	}

	/**
	 * Return the text record serialversionuid.
	 */
	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	/**
	 * Return the text record payload content.
	 */
	public String getPayload() {
		return text;
	}

	/**
	 * Return the the text record timeToLive.
	 */
	public long getTimeToLive() {
		return timeToLive;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}



	@Override
	public int hashCode() {
		return Objects.hash(destination, expiration, messageId, priority,
				redelivered, replyTo, receiveTimestamp, senderTimestamp,
				senderId, timeToLive, properties, text, sequenceNumber);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SolaceTestRecord) {
			SolaceTestRecord other = (SolaceTestRecord) obj;

			boolean p2p = false;
			if (properties != null) {
				p2p = properties.equals(other.properties);
			} else {
				if (other.properties == null) {
					p2p = true;
				}
			}

			return p2p && destination.equals(other.destination)
					&& redelivered == other.redelivered
					&& expiration == other.expiration
					&& priority == other.priority
					&& text.equals(other.text);

		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(",\n\t", SolaceTestRecord.class.getSimpleName() + "[", "]")
				.add("properties=" + properties)
				.add("text='" + text + "'")
				.add("destination='" + destination + "'")
				.add("expiration=" + expiration)
				.add("messageId=" + messageId)
				.add("priority=" + priority)
				.add("redelivered=" + redelivered)
				.add("replyTo='" + replyTo + "'")
				.add("receiveTimestamp=" + receiveTimestamp)
				.add("senderTimestamp=" + senderTimestamp)
				.add("senderId='" + senderId + "'")
				.add("timeToLive=" + timeToLive)
				.add("sequenceNumber=" + sequenceNumber)
				.toString();
	}

	public static Coder<SolaceTestRecord> getCoder() {
		return SerializableCoder.of(SolaceTestRecord.class);
	}

	private static final transient Mapper mapperInstance = new Mapper();
	public static Mapper getMapper() {
		return mapperInstance;
	}

	public static class Mapper implements SolaceIO.InboundMessageMapper<SolaceTestRecord> {
		private static final long serialVersionUID = 42L;

		@Override
		public SolaceTestRecord map(BytesXMLMessage msg) throws Exception {
			Map<String, Object> properties = null;
			SDTMap map = msg.getProperties();
			if (map != null) {
				properties = new HashMap<>();
				for (String key : map.keySet()) {
					properties.put(key, map.get(key));
				}
			}
			String msgData = "";
			if (msg.getContentLength() != 0) {
				msgData += new String(msg.getBytes(), StandardCharsets.UTF_8);
			}
			if (msg.getAttachmentContentLength() != 0) {
				msgData += new String(msg.getAttachmentByteBuffer().array(), StandardCharsets.UTF_8);
			}

			return new SolaceTestRecord(
					properties,
					msgData,
					msg.getDestination().getName(),
					msg.getExpiration(),
					msg.getMessageIdLong(),
					msg.getPriority(),
					msg.getRedelivered(),
					// null means no replyto property
					(msg.getReplyTo() != null) ? msg.getReplyTo().getName() : null,
					msg.getReceiveTimestamp(),
					// 0 means no SenderTimestamp
					(msg.getSenderTimestamp() != null) ? msg.getSenderTimestamp() : 0,
					msg.getSenderId(),
					msg.getTimeToLive(),
					msg.getSequenceNumber());
		}
	}

}