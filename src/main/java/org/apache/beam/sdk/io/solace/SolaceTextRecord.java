package org.apache.beam.sdk.io.solace;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTMap;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;

import java.nio.charset.StandardCharsets;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SolaceTextRecord implements Serializable {
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

  /**
   * Define a new Solace text record.
   */
  public SolaceTextRecord(String destination, long expiration, long messageId, 
      int priority, boolean redelivered, String replyTo, long receiveTimestamp, 
      long senderTimestamp, String senderId, long timeToLive,
      Map<String, Object> properties, String text) {
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
    this.properties = properties;
    this.text = text;
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

  @Override
  public int hashCode() {
    return Objects.hash(destination, expiration, messageId, priority, 
        redelivered, replyTo, receiveTimestamp, senderTimestamp, 
        senderId, timeToLive, properties, text);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SolaceTextRecord) {
      SolaceTextRecord other = (SolaceTextRecord) obj;

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

  public static Coder<SolaceTextRecord> getCoder() {
    return SerializableCoder.of(SolaceTextRecord.class);
  }

  public static Mapper getMapper() {
    return new Mapper();
  }

  public static class Mapper implements SolaceIO.MessageMapper<SolaceTextRecord> {
    private static final long serialVersionUID = 42L;

    @Override
    public SolaceTextRecord mapMessage(BytesXMLMessage msg) throws Exception {
      Map<String, Object> properties = null;
      SDTMap map = msg.getProperties();
      if (map != null) {
        properties = new HashMap<>();
        for (String key : map.keySet()) {
          properties.put(key, map.get(key));
        }
      }

      return new SolaceTextRecord(msg.getDestination().getName(), msg.getExpiration(), 
          msg.getMessageIdLong(), msg.getPriority(), msg.getRedelivered(),
          // null means no replyto property
          (msg.getReplyTo() != null) ? msg.getReplyTo().getName() : null, msg.getReceiveTimestamp(),
          // 0 means no SenderTimestamp
          (msg.getSenderTimestamp() != null) ? msg.getSenderTimestamp() : 0, msg.getSenderId(), 
          msg.getTimeToLive(), properties, new String(msg.getBytes(), StandardCharsets.UTF_8));
    }
  }

}