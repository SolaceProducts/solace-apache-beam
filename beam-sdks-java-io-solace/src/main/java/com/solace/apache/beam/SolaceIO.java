package com.solace.apache.beam;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

import com.solacesystems.jcsmp.BytesXMLMessage;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.Serializable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class SolaceIO {

	//private static final Logger LOG = LoggerFactory.getLogger(SolaceIO.class);

	/**
	 * Read Solace message, the user must set the message mapper and coder.
	 */
	public static <T> Read<T> read(JCSMPProperties jcsmpProperties, List<String> queues, Coder<T> coder,
								   InboundMessageMapper<T> inboundMessageMapper) {
		return new AutoValue_SolaceIO_Read.Builder<T>()
				.setJcsmpProperties(jcsmpProperties)
				.setQueues(queues)
				.setCoder(coder)
				.setInboundMessageMapper(inboundMessageMapper)
				.setAdvanceTimeoutInMillis(500)
				.setMaxNumRecords(Long.MAX_VALUE)
				.setUseSenderMessageId(false)
				.setUseSenderTimestamp(false)
				.build();
	}

	/**
	 * Read Solace message as a STRING.
	 */
	public static Read<String> readString(JCSMPProperties jcsmpProperties, List<String> queues) {
		return SolaceIO.read(jcsmpProperties, queues, StringUtf8Coder.of(), new StringMessageMapper());
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

		abstract boolean useSenderMessageId();

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

			abstract Builder<T> setUseSenderMessageId(boolean useSenderMessageId);


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

		public Read<T> withJcsmpProperties(JCSMPProperties jcsmpProperties) {
			return builder().setJcsmpProperties(jcsmpProperties).build();
		}

		public Read<T> withQueues(List<String> queues) {
			return builder().setQueues(queues).build();
		}

		public Read<T> withUseSenderTimestamp(boolean useSenderTimestamp) {
			return builder().setUseSenderTimestamp(useSenderTimestamp).build();
		}

		public Read<T> withUseSenderMessageId(boolean useSenderMessageId) {
			return builder().setUseSenderMessageId(useSenderMessageId).build();
		}

		public Read<T> withAdvanceTimeoutInMillis(int advanceTimeoutInMillis) {
			return builder().setAdvanceTimeoutInMillis(advanceTimeoutInMillis).build();
		}

		/**
		 * Define the max number of records received by the {@link Read}. When this max
		 * number of records is lower than {@code Long.MAX_VALUE}, the {@link Read} will
		 * provide a bounded {@link PCollection}.
		 */
		public Read<T> withMaxNumRecords(long maxNumRecords) {
			return builder().setMaxNumRecords(maxNumRecords).build();
		}

		/**
		 * Define the max read time (duration) while the {@link Read} will receive
		 * messages. When this max read time is not null, the {@link Read} will provide
		 * a bounded {@link PCollection}.
		 */
		public Read<T> withMaxReadTime(Duration maxReadTime) {
			return builder().setMaxReadTime(maxReadTime).build();
		}

		public Read<T> withInboundMessageMapper(InboundMessageMapper<T> inboundMessageMapper) {
			return builder().setInboundMessageMapper(inboundMessageMapper).build();
		}

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
			builder.add(DisplayData.item("useSenderMessageId", useSenderMessageId()));
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
			for (String propertyName : new String[]{JCSMPProperties.HOST, JCSMPProperties.USERNAME, JCSMPProperties.PASSWORD,
					JCSMPProperties.VPN_NAME}) {
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
