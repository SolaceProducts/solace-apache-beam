/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.solace.apache.beam.examples;

import com.solace.apache.beam.SolaceIO;
import com.solace.apache.beam.examples.common.SolaceByteBuffRecord;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * An example that counts words in text, and can run over either unbounded or bounded input
 * collections.
 *
 * <p>This class, {@link WindowedWordCount}, is the last in a series of four successively more
 * detailed 'word count' examples. First take a look at {@link MinimalWordCount}, {@link WordCount},
 * and {@link DebuggingWordCount}.
 *
 * <p>Basic concepts, also in the MinimalWordCount, WordCount, and DebuggingWordCount examples:
 * Reading text files; counting a PCollection; writing to GCS; executing a Pipeline both locally and
 * using a selected runner; defining DoFns; user-defined PTransforms; defining PipelineOptions.
 *
 * <p>New Concepts:
 *
 * <pre>
 *   1. Unbounded and bounded pipeline input modes
 *   2. Adding timestamps to data
 *   3. Windowing
 *   4. Re-using PTransforms over windowed PCollections
 *   5. Accessing the window of an element
 *   6. Writing data to per-window text files
 * </pre>
 *
 * <p>By default, the examples will run with the {@code DirectRunner}. To change the runner,
 * specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 * <p>
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 * <p>To execute this pipeline locally, specify a local output file (if using the {@code
 * DirectRunner}) or output prefix on a supported distributed file system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with {@code --inputFile}.
 *
 * <p>By default, the pipeline will do fixed windowing, on 10-minute windows. You can change this
 * interval by setting the {@code --windowSize} parameter, e.g. {@code --windowSize=15} for
 * 15-minute windows.
 *
 * <p>The example will try to cancel the pipeline on the signal to terminate the process (CTRL-C).
 */
public class SolaceProtoBuffRecordTest {
	private static final Logger LOG = LoggerFactory.getLogger(SolaceProtoBuffRecordTest.class);

	public interface Options
			extends WordCount.WordCountOptions {
		@Description("IP and port of the client appliance. (e.g. -cip=192.168.160.101)")
		String getCip();

		void setCip(String value);

		@Description("VPN name")
		String getVpn();

		void setVpn(String value);

		@Description("Client username")
		String getCu();

		void setCu(String value);

		@Description("Client password (default '')")
		@Default.String("")
		String getCp();

		void setCp(String value);

		@Description("List of queues for subscribing")
		String getSql();

		void setSql(String value);

		@Description("Enable reading sender timestamp to determine freshness of data")
		@Default.Boolean(false)
		boolean getSts();

		void setSts(boolean value);

		@Description("Enable reading sender sequence number to determine duplication of data")
		@Default.Boolean(false)
		boolean getSmi();

		void setSmi(boolean value);

		@Description("The timeout in milliseconds while try to receive a messages from Solace broker")
		@Default.Integer(100)
		int getTimeout();

		void setTimeout(int timeoutInMillis);
	}

	static void runPublishProtoBuff(Options options) throws Exception {

		JCSMPProperties jcsmpProperties = new JCSMPProperties();
		jcsmpProperties.setProperty(JCSMPProperties.HOST, options.getCip());
		jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, options.getVpn());
		jcsmpProperties.setProperty(JCSMPProperties.USERNAME, options.getCu());
		jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, options.getCp());

		TestOuterClass.Test.Builder test = TestOuterClass.Test.newBuilder();
		String body = "Test Message";
		test.setTest(body);
		List<String> queues = Arrays.asList(options.getSql().split(","));

		final JCSMPSession session =  JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
		session.connect();

		for (String queueName : queues) {
    		final XMLMessageProducer prod = session.getMessageProducer(
            new JCSMPStreamingPublishEventHandler() {

                @Override
                public void responseReceived(String messageId) {
					LOG.debug("Producer received ack for msg ID {} ", messageId);
                }
                @Override
                public void handleError(String messageId, JCSMPException e, long timestamp) {
					LOG.warn("Producer received error for msg ID {} @ {} - {}", messageId ,timestamp, e);

                }
            });

			Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			// Publish-only session is now hooked up and running!
			LOG.info("Connected. About to send message to queue {}...",queue.getName());

			BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
			msg.setDeliveryMode(DeliveryMode.PERSISTENT);
			msg.setData(test.build().toByteArray());
			// Send message directly to the queue
			for (int x =1; x < 1000; x++) {
				prod.send(msg, queue);
			}

			LOG.info("Messages sent to queue {}...",queue.getName());

		}

		session.closeSession();

	}


	static PipelineResult runPaseProtoBuff(Options options) throws Exception {


		List<String> queues = Arrays.asList(options.getSql().split(","));
		JCSMPProperties jcsmpProperties = new JCSMPProperties();
		jcsmpProperties.setProperty(JCSMPProperties.HOST, options.getCip());
		jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, options.getVpn());
		jcsmpProperties.setProperty(JCSMPProperties.USERNAME, options.getCu());
		jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, options.getCp());
		Pipeline pipeline = Pipeline.create(options);

		pipeline
			.apply(SolaceIO.read(jcsmpProperties, queues, SolaceByteBuffRecord.getCoder(), SolaceByteBuffRecord.getMapper())
				.withUseSenderTimestamp(options.getSts())
				.withAdvanceTimeoutInMillis(options.getTimeout())
			)
			.apply(
				Window.<SolaceByteBuffRecord>into(FixedWindows.of(Duration.standardSeconds(4)))
			)
			.apply(Distinct
				.<SolaceByteBuffRecord, Long>withRepresentativeValueFn(SolaceByteBuffRecord::getMessageId)
				.withRepresentativeType(TypeDescriptor.of(Long.class))
			)
			.apply(MapElements.via(new InferableFunction<SolaceByteBuffRecord, TestOuterClass.Test>() {
				@Override
				public TestOuterClass.Test apply(SolaceByteBuffRecord deduppedInput) throws Exception {
					return TestOuterClass.Test.parseFrom(deduppedInput.getRawProtoBuff());
				}
			}))
			.apply(MapElements.into(TypeDescriptors.nulls()).via((TestOuterClass.Test x) -> {
				LOG.info(x.toString());
				return null;
			}));


		return pipeline.run();


	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		try {
			runPublishProtoBuff(options);
			PipelineResult result = runPaseProtoBuff(options);
			result.waitUntilFinish();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
