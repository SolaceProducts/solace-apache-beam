package com.solace.connector.beam.examples.common;

import com.solace.connector.beam.SolaceIO;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;
import java.util.Objects;
import java.util.Arrays;

public class SolaceByteBuffRecord implements Serializable {
	private static final long serialVersionUID = 42L;

	private final long messageId;
	private final byte[] rawProtoBuff;

	/**
	 * Define a new Solace ByteBuff record.
	 */
	public SolaceByteBuffRecord(long messageId, byte[] rawProtoBuff) {
		this.messageId = messageId;
		this.rawProtoBuff = rawProtoBuff;
	}

	/**
	 * Return the protoBuff record messageId.
	 */
	public Long getMessageId() {
		return new Long(messageId);
	}

	/**
	 * Return the protoBuff record raw buffer.
	 */
	public byte[] getRawProtoBuff() {
		return rawProtoBuff;
	}

	@Override
	public int hashCode() {
		return Objects.hash(messageId, rawProtoBuff.toString());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SolaceByteBuffRecord) {
			SolaceByteBuffRecord other = (SolaceByteBuffRecord) obj;

			return messageId == other.getMessageId()
				&& Arrays.equals(rawProtoBuff, other.getRawProtoBuff());

		} else {
			return false;
		}
	}

	public static Coder<SolaceByteBuffRecord> getCoder() {
		return SerializableCoder.of(SolaceByteBuffRecord.class);
	}

	public static Mapper getMapper() {
		return new Mapper();
	}

	public static class Mapper implements SolaceIO.InboundMessageMapper<SolaceByteBuffRecord> {
		private static final long serialVersionUID = 42L;

		@Override
		public SolaceByteBuffRecord map(BytesXMLMessage msg) throws Exception {
			BytesMessage bMsg = (BytesMessage)msg;
			return new SolaceByteBuffRecord(
					bMsg.getMessageIdLong(),
					bMsg.getData());
		}
	}

}