package com.solace.connector.beam;

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class SolaceIO {

	//private static final Logger LOG = LoggerFactory.getLogger(SolaceIO.class);
	private static final int DEFAULT_ADVANCE_TIMEOUT = 500;
	private static final boolean DEFAULT_USE_SENDER_TIMESTAMP = false;

	/**
	 * Read messages from a single Solace PubSub+ broker using JCSMP.
	 * @param jcsmpProperties see {@link Read#withJcsmpProperties}
	 * @param queues see {@link Read#withQueues}
	 * @param coder see {@link Read#withCoder}
	 * @param inboundMessageMapper see {@link Read#withInboundMessageMapper}
	 * @param <T> The type of the resulting elements for the output {@link PCollection}
	 * @return a new {@link Read} with default configuration
	 */
	public static <T> Read<T> read(JCSMPProperties jcsmpProperties, List<String> queues, Coder<T> coder,
								   InboundMessageMapper<T> inboundMessageMapper) {
		return new AutoValue_SolaceIO_Read.Builder<T>()
				.setJcsmpProperties(jcsmpProperties)
				.setQueues(queues)
				.setCoder(coder)
				.setInboundMessageMapper(inboundMessageMapper)
				.setAdvanceTimeoutInMillis(DEFAULT_ADVANCE_TIMEOUT)
				.setMaxNumRecords(Long.MAX_VALUE)
				.setUseSenderTimestamp(DEFAULT_USE_SENDER_TIMESTAMP)
				.build();
	}

	private SolaceIO() {
	}

	/**
	 * An interface used by {@link SolaceIO.Read} for converting each Solace Message
	 * into an element of the resulting {@link PCollection}.
	 */
	@FunctionalInterface
	public interface InboundMessageMapper<T> extends Serializable {
		T map(BytesXMLMessage message) throws Exception;
	}

	/**
	 * A {@link PTransform} to read from a Solace broker.
	 */
	@AutoValue
	public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
		private static final long serialVersionUID = 42L;

		abstract Builder<T> builder();

		abstract JCSMPProperties jcsmpProperties();

		abstract List<String> queues();

		abstract boolean useSenderTimestamp();

		// The timeout in milliseconds while try to receive a messages from Solace broker
		abstract int advanceTimeoutInMillis();

		abstract long maxNumRecords();

		@Nullable
		abstract Duration maxReadTime();

		abstract InboundMessageMapper<T> inboundMessageMapper();

		abstract Coder<T> coder();

		@AutoValue.Builder
		abstract static class Builder<T> {
			abstract Builder<T> setJcsmpProperties(JCSMPProperties jcsmpProperties);

			abstract Builder<T> setQueues(List<String> queues);

			abstract Builder<T> setUseSenderTimestamp(boolean useSenderTimestamp);

			abstract Builder<T> setAdvanceTimeoutInMillis(int timeoutInMillis);

			abstract Builder<T> setMaxNumRecords(long maxNumRecords);

			abstract Builder<T> setMaxReadTime(Duration maxReadTime);

			abstract Builder<T> setInboundMessageMapper(InboundMessageMapper<T> inboundMessageMapper);

			abstract Builder<T> setCoder(Coder<T> coder);

			abstract Read<T> autoBuild();

			public Read<T> build() {
				Read<T> read = autoBuild();

				read.validateConfig();

				return read;
			}
		}

		/**
		 * Sets the JCSMP connection config for Solace PubSub+.
		 *
		 * <p>Note: the JCSMP property {@link JCSMPProperties#CLIENT_NAME} must be {@code null}. This is because each
		 * Apache Beam split has its own Solace session, and multiple sessions cannot share the same client name.
		 * @param jcsmpProperties Solace PubSub+ JCSMP connection config
		 * @return a new copy of this {@link Read} configured with the provided JCSMP
		 */
		public Read<T> withJcsmpProperties(JCSMPProperties jcsmpProperties) {
			return builder().setJcsmpProperties(jcsmpProperties).build();
		}

		/**
		 * Sets the list of pre-configured queues to which this {@link Read} will consume messages from.
		 *
		 * <p>For non-exclusive queues, you may add duplicate queue names in this list to create additional concurrent
		 * readers for them.
		 * @param queues list of queues
		 * @return a new copy of this {@link Read} configured with the provided queue list
		 */
		public Read<T> withQueues(List<String> queues) {
			return builder().setQueues(queues).build();
		}

		/**
		 * Sets whether or not for this {@link Read} to use the sender timestamp to determine the freshness of its
		 * data. Otherwise, the time at which Beam receives the data will be used.
		 *
		 * <p>By default, the latency measurement is taken from the time the message enters Dataflow and does not take
		 * into account the time sitting in a Solace queue waiting to be processed. If messages are published with
		 * sender timestamps and useSenderTimestamp is enabled in the SolaceIO, then end to end latencies will be used
		 * and reported. For java clients the JCSMP property {@link JCSMPProperties#GENERATE_SEND_TIMESTAMPS} will
		 * ensure that each message is sent with a timestamp.
		 *
		 * <p>Default: {@value DEFAULT_USE_SENDER_TIMESTAMP}
		 * @param useSenderTimestamp set to true to use the data's sender timestamp to determine their freshness
		 * @return a new copy of this {@link Read} configured with the provided indication to use or not use sender
		 * timestamps
		 */
		public Read<T> withUseSenderTimestamp(boolean useSenderTimestamp) {
			return builder().setUseSenderTimestamp(useSenderTimestamp).build();
		}

		/**
		 * Sets the message polling timeout for this {@link Read}. If the poll timeout is passed, then this
		 * {@link Read} will treat that poll as a {@code null} message. i.e. no messages were available from the Solace PubSub+
		 * source.
		 *
		 * <p>Default: {@value #DEFAULT_ADVANCE_TIMEOUT}
		 * @param advanceTimeoutInMillis the message polling timeout in milliseconds
		 * @return a new copy of this {@link Read} configured with the provided advance timeout
		 */
		public Read<T> withAdvanceTimeoutInMillis(int advanceTimeoutInMillis) {
			return builder().setAdvanceTimeoutInMillis(advanceTimeoutInMillis).build();
		}

		/**
		 * Sets the max number of records received by this {@link Read}. When this max
		 * number of records is lower than {@link Long#MAX_VALUE}, the {@link Read} will
		 * provide a bounded {@link PCollection}.
		 *
		 * <p>Default: {@link Long#MAX_VALUE}
		 * @param maxNumRecords the maximum number of records to receive
		 * @return a new copy of this {@link Read} configured with the provided maximum number of records
		 */
		public Read<T> withMaxNumRecords(long maxNumRecords) {
			return builder().setMaxNumRecords(maxNumRecords).build();
		}

		/**
		 * Sets the max read time (duration) that this {@link Read} will receive messages.
		 * When this max read time is not {@code null}, the {@link Read} will provide a bounded {@link PCollection}.
		 * @param maxReadTime the maximum time to receive messages
		 * @return a new copy of this {@link Read} configured with the provided maximum read time
		 */
		public Read<T> withMaxReadTime(Duration maxReadTime) {
			return builder().setMaxReadTime(maxReadTime).build();
		}

		/**
		 * Sets the {@link InboundMessageMapper} that this {@link Read} will use to map Solace messages into elements
		 * of the resulting {@link PCollection}.
		 * @param inboundMessageMapper the message mapper
		 * @return a new copy of this {@link Read} configured with the provided mapper
		 * @see InboundMessageMapper
		 */
		public Read<T> withInboundMessageMapper(InboundMessageMapper<T> inboundMessageMapper) {
			return builder().setInboundMessageMapper(inboundMessageMapper).build();
		}

		/**
		 * Sets the {@link Coder} that this {@link Read} will use to encode and decode the elements of the resulting
		 * {@link PCollection}.
		 * @param coder the output coder
		 * @return a new copy of this {@link Read} configured with the provided output coder
		 * @see Coder
		 */
		public Read<T> withCoder(Coder<T> coder) {
			return builder().setCoder(coder).build();
		}

		@Override
		public PCollection<T> expand(PBegin input) {
			validateConfig();

			org.apache.beam.sdk.io.Read.Unbounded<T> unbounded = org.apache.beam.sdk.io.Read
					.from(new UnboundedSolaceSource<>(this));

			PTransform<PBegin, PCollection<T>> transform = unbounded;

			if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
				transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
			}

			return input.getPipeline().apply(transform);
		}

		@Override
		public void populateDisplayData(DisplayData.Builder builder) {
			super.populateDisplayData(builder);
			builder.add(DisplayData.item("queues", String.join("\n", queues())));
			builder.add(DisplayData.item("useSenderTimestamp", useSenderTimestamp()));
			builder.add(DisplayData.item("advanceTimeoutInMillis", advanceTimeoutInMillis()));
			builder.addIfNotDefault(DisplayData.item("maxNumRecords", maxNumRecords()), Long.MAX_VALUE);
			builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));

			for (String propertyName : jcsmpProperties().propertyNames()) {
				Set<String> hiddenProperties = new HashSet<>();
				hiddenProperties.add(JCSMPProperties.PASSWORD);
				hiddenProperties.add(JCSMPProperties.SSL_KEY_STORE_PASSWORD);
				hiddenProperties.add(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD);
				hiddenProperties.add(JCSMPProperties.SSL_TRUST_STORE_PASSWORD);
				if (hiddenProperties.contains(propertyName) || propertyName.toLowerCase().contains("password"))
					continue;

				Object propertyValue = jcsmpProperties().getProperty(propertyName);
				if (propertyValue == null) continue;

				Optional<DisplayData.Type> type = Optional.ofNullable(DisplayData.inferType(propertyValue));
				if (!type.isPresent()) {
					type = Optional.of(DisplayData.Type.STRING);
					propertyValue = propertyValue.toString();
				}
				builder.addIfNotNull(DisplayData.item("jcsmpProperties." + propertyName, type.get(), propertyValue));
			}
		}

		private void validateConfig() {
			// Validate jcsmpProperties
			checkArgument(jcsmpProperties() != null, "jcsmpProperties cannot be null");
			checkArgument(jcsmpProperties().getStringProperty(JCSMPProperties.CLIENT_NAME) == null,
					String.format("jcmspProperties property %s must be null", JCSMPProperties.CLIENT_NAME));
			for (String propertyName : new String[]{
					JCSMPProperties.HOST, JCSMPProperties.USERNAME, JCSMPProperties.PASSWORD, JCSMPProperties.VPN_NAME}) {
				checkArgument(jcsmpProperties().getStringProperty(propertyName) != null &&
								!jcsmpProperties().getStringProperty(propertyName).isEmpty(),
						String.format("jcsmpProperties property %s cannot be null", propertyName));
			}

			// Validate queues
			checkArgument(queues() != null && !queues().isEmpty(), "queues cannot be null or empty");

			// Validate inboundMessageMapper
			checkArgument(inboundMessageMapper() != null, "inboundMessageMapper cannot be null");

			// Validate coder
			checkArgument(coder() != null, "coder cannot be null");

			// Validate advanceTimeoutInMillis
			checkArgument(advanceTimeoutInMillis() > 0, "advanceTimeoutInMillis must be greater than 0");

			// Validate maxNumRecords
			checkArgument(maxNumRecords() > 0, "maxNumRecords must be greater than 0");

			// Validate maxReadTime
			if (maxReadTime() != null) {
				checkArgument(maxReadTime().isLongerThan(Duration.ZERO), "maxReadTime must be greater than 0");
			}
		}
	}

}
