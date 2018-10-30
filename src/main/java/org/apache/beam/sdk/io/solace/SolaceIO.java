package org.apache.beam.sdk.io.solace;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.Queue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


import static com.google.common.base.Preconditions.checkArgument;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class SolaceIO {

    private static final Logger LOG = LoggerFactory.getLogger(SolaceIO.class);

    // read message from Solace as String
    public static Read<String> readAsString() {
        return new AutoValue_SolaceIO_Read.Builder<String>()
                .setMaxReadTime(null)
                .setMaxNumRecords(Long.MAX_VALUE)
                .setCoder(StringUtf8Coder.of())
                .setMessageMapper(new StringMessageMapper())
                .build();
    }

    // read message from Solace, the user should must set the message maper and coder
    public static <T> Read<T> readMessage() {
        return new AutoValue_SolaceIO_Read.Builder<T>().setMaxNumRecords(Long.MAX_VALUE).build();
      }    

    private SolaceIO() {}

  /**
   * An interface used by {@link SolaceIO.Read} for converting each Solace Message into an element
   * of the resulting {@link PCollection}.
   */
  @FunctionalInterface
  public interface MessageMapper<T> extends Serializable {
    T mapMessage(BytesXMLMessage message) throws Exception;
  }


    /** A POJO describing a Solace SMF connection. */
    @AutoValue
    public abstract static class ConnectionConfiguration implements Serializable {
        private static final long serialVersionUID = 42L;

        @Nullable
        abstract String getHost();

        @Nullable
        abstract List<String> getQueues();

        @Nullable
        abstract String getClientName();

        @Nullable
        abstract String getVpn();

        @Nullable
        abstract String getUsername();

        @Nullable
        abstract String getPassword();
        
        abstract boolean isAutoAck();

        abstract Builder builder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setHost(String host);


            abstract Builder setQueues(List<String> queues);

            abstract Builder setClientName(String clientName);

            abstract Builder setVpn(String vpn);

            abstract Builder setUsername(String username);

            abstract Builder setPassword(String password);

            abstract Builder setAutoAck(boolean autoAck);

            abstract ConnectionConfiguration build();
        }

        public static ConnectionConfiguration create(String host, List<String> queues) {
            checkArgument(host != null, "host can not be null");
            checkArgument(queues != null && queues.size()>0, "queues can not be null or empty");

            return new AutoValue_SolaceIO_ConnectionConfiguration.Builder()
                    .setHost(host)
                    .setQueues(queues)
                    .setAutoAck(false)
                    .build();
        }

        public ConnectionConfiguration withVpn(String vpn) {
            return builder().setVpn(vpn).build();
        }

        public ConnectionConfiguration withUsername(String username) {
            ConnectionConfiguration.Builder b = builder();
            String[] args = username.split("@");
            if (args.length == 2){
                b = b.setVpn(args[1]);
            } else {
                b = b.setVpn("default");
            }

            return b.setUsername(args[0]).build();
        }

        public ConnectionConfiguration withPassword(String password) {
            return builder().setPassword(password).build();
        }

        public ConnectionConfiguration withAutoAck(boolean autoAck) {
            return builder().setAutoAck(autoAck).build();
        }

        private void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("host", getHost()));
            builder.add(DisplayData.item("vpn", getVpn()));
            builder.addIfNotNull(DisplayData.item("clientName", getClientName()));
            builder.addIfNotNull(DisplayData.item("username", getUsername()));
        }

        private JCSMPSession createSolaceSession() throws Exception {
            final JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, getHost());     // host:port
            properties.setProperty(JCSMPProperties.USERNAME, getUsername()); // client-username
            properties.setProperty(JCSMPProperties.PASSWORD, getPassword()); // client-password
            properties.setProperty(JCSMPProperties.VPN_NAME,  getVpn()); // message-vpn

            if (getClientName() != null) {
                properties.setProperty(JCSMPProperties.CLIENT_NAME,  getClientName()); // message-vpn
            }

            final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);

            LOG.debug("Creating Solace session to {}", getHost());
            return session;
        }
    }

    /** A {@link PTransform} to read from a Solace broker. */
    @AutoValue
    public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
        private static final long serialVersionUID = 42L;

        @Nullable
        abstract ConnectionConfiguration connectionConfiguration();

        abstract long maxNumRecords();

        @Nullable
        abstract Duration maxReadTime();

        @Nullable
        abstract MessageMapper<T> messageMapper();

        @Nullable
        abstract Coder<T> coder();
    
        abstract Builder<T> builder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setConnectionConfiguration(ConnectionConfiguration config);

            abstract Builder<T> setMaxNumRecords(long maxNumRecords);

            abstract Builder<T> setMaxReadTime(Duration maxReadTime);

            abstract Builder<T> setMessageMapper(MessageMapper<T> mesageMapper);

            abstract Builder<T> setCoder(Coder<T> coder);

            abstract Read<T> build();
        }

        public Read<T> withConnectionConfiguration(ConnectionConfiguration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return builder().setConnectionConfiguration(configuration).build();
        }

        /**
         * Define the max number of records received by the {@link Read}. When this max number of
         * records is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
         * PCollection}.
         */
        public Read<T> withMaxNumRecords(long maxNumRecords) {
            return builder().setMaxNumRecords(maxNumRecords).build();
        }

        /**
         * Define the max read time (duration) while the {@link Read} will receive messages. When this
         * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
         */
        public Read<T> withMaxReadTime(Duration maxReadTime) {
            return builder().setMaxReadTime(maxReadTime).build();
        }

        public Read<T> withMessageMapper(MessageMapper<T> messageMapper) {
            checkArgument(messageMapper != null, "messageMapper can not be null");
            return builder().setMessageMapper(messageMapper).build();
        }
           
        public Read<T> withCoder(Coder<T> coder) {
        checkArgument(coder != null, "coder can not be null");
        return builder().setCoder(coder).build();
        }
      
        @Override
        public PCollection<T> expand(PBegin input) {
            checkArgument(connectionConfiguration() != null, "withConnectionConfiguration() is required");
            checkArgument(messageMapper() != null, "withMessageMapper() is required");
            checkArgument(coder() != null, "withCoder() is required");

            org.apache.beam.sdk.io.Read.Unbounded<T> unbounded =
                    org.apache.beam.sdk.io.Read.from(new UnboundedSolaceSource<T>(this));

            PTransform<PBegin, PCollection<T>> transform = unbounded;

            if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
                transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
            }

            return input.getPipeline().apply(transform);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            connectionConfiguration().populateDisplayData(builder);
            if (maxNumRecords() != Long.MAX_VALUE) {
                builder.add(DisplayData.item("maxNumRecords", maxNumRecords()));
            }
            builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));
        }
    }

    /**
     * Checkpoint for an unbounded Solace source. Consists of the Solace messages waiting to be
     * acknowledged and oldest pending message timestamp.
     */
    @VisibleForTesting
    static class SolaceCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
        private static final long serialVersionUID = 42L;

        private String clientName;
        private Instant oldestMessageTimestamp = Instant.now();
        private boolean isAutoAck;
        private transient List<BytesXMLMessage> messages = new ArrayList<>();

        public SolaceCheckpointMark(String name, boolean autoAck){
            clientName = name;
            isAutoAck = autoAck;
          }
      
        public void add(BytesXMLMessage message, Instant timestamp) {
            if (timestamp.isBefore(oldestMessageTimestamp)) {
                oldestMessageTimestamp = timestamp;
            }
            if(!isAutoAck){
                messages.add(message);
            }
        }

        @Override
        public void finalizeCheckpoint() {
            LOG.debug("Finalizing checkpoint acknowledging {} pending messages for client name {}", messages.size(), clientName);

            for (BytesXMLMessage message : messages) {
                try {
                    message.ackMessage();
                } catch (Exception e) {
                    LOG.warn("Can't ack message for client name {}", clientName);
                }
            }

            oldestMessageTimestamp = Instant.now();
            messages.clear();
        }

        // set an empty list to messages when deserialize
        private void readObject(java.io.ObjectInputStream stream)
                throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
            messages = new ArrayList<>();
        }

        @Override
        public boolean equals(Object other) {
          if (other instanceof SolaceCheckpointMark){
            SolaceCheckpointMark that = (SolaceCheckpointMark)other;
            return this.clientName.equals(that.clientName) && 
                this.oldestMessageTimestamp.equals(that.oldestMessageTimestamp) &&
                (this.isAutoAck == that.isAutoAck);
          }else{
            return false;
          }
        }
    
        @Override
        public int hashCode() {
          // Effective Java Item 11
          int r = clientName.hashCode()*31 + oldestMessageTimestamp.hashCode();
          r = r * 31 + Boolean.hashCode(isAutoAck);
          return r;
        }
    
    }

    @VisibleForTesting
    static class UnboundedSolaceSource<T> extends UnboundedSource<T, SolaceCheckpointMark> {
        private static final long serialVersionUID = 42L;

        private final Read<T> spec;
        private String queueName;

        public String getQueueName() {
            return queueName;
        }

        public UnboundedSolaceSource(Read<T> spec) {
            this.spec = spec;
        }

        @Override
        public UnboundedReader<T> createReader(
                PipelineOptions options, SolaceCheckpointMark checkpointMark) {
            return new UnboundedSolaceReader<T>(this, checkpointMark);
        }

        @Override
        public List<UnboundedSolaceSource<T>> split(int desiredNumSplits, PipelineOptions options) {
            ConnectionConfiguration cc = spec.connectionConfiguration();
            LOG.debug("Queue Numbers: {}, desiredNumSplits: {}, PipelineOptions: {}",
                    cc.getQueues().size(), desiredNumSplits, options);

            List<UnboundedSolaceSource<T>> sourceList = new ArrayList<>();
            for (String queueName : cc.getQueues()){
                UnboundedSolaceSource<T> source = new UnboundedSolaceSource<T>(spec);
                source.queueName = queueName;
                sourceList.add(source);
            }

            return sourceList;
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            spec.populateDisplayData(builder);
        }

        @Override
        public Coder<SolaceCheckpointMark> getCheckpointMarkCoder() {
            return SerializableCoder.of(SolaceCheckpointMark.class);
        }

        @Override
        public Coder<T> getOutputCoder() {
            return this.spec.coder();
        }
    }

    /**
     * Unbounded Reader to read messages from a Solace Router.
     */
    @VisibleForTesting
    static class UnboundedSolaceReader<T> extends UnboundedSource.UnboundedReader<T> {

        private final UnboundedSolaceSource<T> source;

        private JCSMPSession session;
        private FlowReceiver flowReceiver;
        private String clientName;
        private T current;
        private Instant currentTimestamp;
        private SolaceCheckpointMark checkpointMark;

        public UnboundedSolaceReader(UnboundedSolaceSource<T> source, SolaceCheckpointMark checkpointMark) {
            this.source = source;
            this.current = null;
        }

        @Override
        public boolean start() throws IOException {
            Read<T> spec = source.spec;
            try {
                session = spec.connectionConfiguration().createSolaceSession();
                clientName = (String)session.getProperty(JCSMPProperties.CLIENT_NAME);
                checkpointMark = new SolaceCheckpointMark(clientName,
                    source.spec.connectionConfiguration().isAutoAck());

                LOG.debug("Solace client name is {}", clientName);
                session.connect();

                // do NOT provision the queue, so "Unknown Queue" exception will be threw if the
                // queue is not existed already
                final Queue queue = JCSMPFactory.onlyInstance().createQueue(source.getQueueName());

                // Create a Flow be able to bind to and consume messages from the Queue.
                final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
                flow_prop.setEndpoint(queue);

                if (spec.connectionConfiguration().isAutoAck()){
                    // auto ack the messages
                    flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
                } else {
                    // will ack the messages in checkpoint
                    flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
                }


                EndpointProperties endpoint_props = new EndpointProperties();
                endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
                // bind to the queue, passing null as message listener for no async callback
                flowReceiver = session.createFlow(null, flow_prop, endpoint_props);
                // Start the consumer
                flowReceiver.start();
                LOG.debug("Starting Solace session [{}] on queue[{}]..."
                        , clientName
                        , source.getQueueName()
                        );

                return advance();

            } catch (Exception e){
                e.printStackTrace();
                throw new IOException(e);
            }
        }

        @Override
        public boolean advance() throws IOException {
            try {
                BytesXMLMessage msg = flowReceiver.receive(1000);  // wait max 1 second for a message

                if (msg == null) {
                    return false;
                }

                current = this.source.spec.messageMapper().mapMessage(msg);

                // TODO: get sender timestamp
                currentTimestamp = Instant.now();
                checkpointMark.add(msg, currentTimestamp);
            } catch (Exception e) {
                throw new IOException(e);
            }
            return true;
        }

        @Override
        public void close() throws IOException {
            LOG.debug("Closing Solace session [{}]", clientName);
            try {
                if (flowReceiver != null) {
                    flowReceiver.close();
                }
                if (session != null){
                    session.closeSession();
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public Instant getWatermark() {
            return checkpointMark.oldestMessageTimestamp;
        }

        @Override
        public UnboundedSource.CheckpointMark getCheckpointMark() {
            SolaceCheckpointMark scm = checkpointMark;
            checkpointMark = new SolaceCheckpointMark(clientName,
            source.spec.connectionConfiguration().isAutoAck());

            return scm;
        }

        @Override
        public T getCurrent() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            return current;
        }

        @Override
        public Instant getCurrentTimestamp() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            return currentTimestamp;
        }

        @Override
        public UnboundedSolaceSource<T> getCurrentSource() {
            return source;
        }
    }

}
