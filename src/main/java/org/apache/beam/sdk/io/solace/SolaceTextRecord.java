package org.apache.beam.sdk.io.solace;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.HashMap;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTMap;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;

public class SolaceTextRecord implements Serializable {
    private static final long serialVersionUID = 42L;

    // Application properties
    private final Map<String, Object> properties;
    private final String    text;

    private final String    destination;
    private final long      expiration;
    private final long      messageId;
    private final int       priority;
    private final boolean   redelivered;
    private final String    replyTo;
    private final long      receiveTimestamp;
    private final long      senderTimestamp;
    private final String    senderId;
    private final long      timeToLive;

    public SolaceTextRecord(
        String    destination,
        long      expiration,
        long      messageId,
        int       priority,
        boolean   redelivered,
        String    replyTo,
        long      receiveTimestamp,
        long      senderTimestamp,
        String    senderId,
        long      timeToLive,
        Map<String, Object> properties,
        String    text
    ){
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
     * @return the destination
     */
    public String getDestination() {
        return destination;
    }

    /**
     * @return the expiration
     */
    public long getExpiration() {
        return expiration;
    }

    /**
     * @return the messageId
     */
    public long getMessageId() {
        return messageId;
    }

    /**
     * @return the priority
     */
    public int getPriority() {
        return priority;
    }

    /**
     * @return the properties
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * @return the redelivered
     */
    public boolean isRedelivered(){
        return redelivered;
    }

    /**
     * @return the receiveTimestamp
     */
    public long getReceiveTimestamp() {
        return receiveTimestamp;
    }

    /**
     * @return the replyTo
     */
    public String getReplyTo() {
        return replyTo;
    }

    /**
     * @return the senderId
     */
    public String getSenderId() {
        return senderId;
    }

    /**
     * @return the senderTimestamp
     */
    public long getSenderTimestamp() {
        return senderTimestamp;
    }

    /**
     * @return the serialversionuid
     */
    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * @return the text
     */
    public String getPayload() {
        return text;
    }

    /**
     * @return the timeToLive
     */
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
        destination,
        expiration,
        messageId,
        priority,
        redelivered,
        replyTo,
        receiveTimestamp,
        senderTimestamp,
        senderId,
        timeToLive,
        properties,
        text);
    }  

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SolaceTextRecord) {
            SolaceTextRecord other = (SolaceTextRecord) obj;

            boolean p2p =false;
            if (properties != null) {
                p2p = properties.equals(other.properties);
            } else {
                if (other.properties == null){
                    p2p = true;
                }
            }

            return p2p
                && destination.equals(other.destination)
                && redelivered == other.redelivered
                && expiration == other.expiration
                && priority == other.priority
                && text.equals(other.text);

        } else {
            return false;
        }
    }
  
    public static Coder<SolaceTextRecord> getCoder(){
        return SerializableCoder.of(SolaceTextRecord.class);
    }

    public static Mapper getMapper(){
        return new Mapper();        
    }

    public static class Mapper implements SolaceIO.MessageMapper<SolaceTextRecord> {
        private static final long serialVersionUID = 42L;

        @Override
        public SolaceTextRecord mapMessage(BytesXMLMessage msg) throws Exception {
            Map<String, Object> properties = null;
            SDTMap map = msg.getProperties();
            if (map != null){
                properties = new HashMap<>();
                for (String key : map.keySet()){
                    properties.put(key, map.get(key));
                }   
            }
            
            return new SolaceTextRecord(
                msg.getDestination().getName(), 
                msg.getExpiration(), 
                msg.getMessageIdLong(),
                msg.getPriority(), 
                msg.getRedelivered(), 
                // null means no replyto property
                (msg.getReplyTo()!=null)?msg.getReplyTo().getName():null,
                msg.getReceiveTimestamp(), 
                // 0 means no SenderTimestamp
                (msg.getSenderTimestamp()!=null)?msg.getSenderTimestamp():0,
                msg.getSenderId(), 
                msg.getTimeToLive(), 
                properties, 
                new String(msg.getBytes(), StandardCharsets.UTF_8));
        }
    }
    
}