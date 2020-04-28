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
package com.solace.connector.beam.examples;

import com.google.common.primitives.Longs;
import com.solace.connector.beam.SolaceIO;
import com.solace.connector.beam.examples.common.CountWords;
import com.solace.connector.beam.examples.common.SolaceTextRecord;
import com.solace.connector.beam.examples.common.WordCountToTextFn;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * An example that counts the number of each word in the received Solace message payloads, outputs the results as
 * log messages, and can run over either unbounded or bounded input collections.
 *
 * <p>By default, the examples will run with the {@code DirectRunner}. To run the pipeline on
 * Google Dataflow, specify:
 *
 * <pre>{@code
 * --runner=DataflowRunner
 * }</pre>
 * <p>
 */
public class SolaceRecordTest {
	private static final Logger LOG = LoggerFactory.getLogger(SolaceRecordTest.class);

	public interface Options extends PipelineOptions {
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

		;
	}

	static void runWindowedWordCount(Options options) throws Exception {

		List<String> queues = Arrays.asList(options.getSql().split(","));
		boolean useSenderMsgId = options.getSmi();

		Pipeline pipeline = Pipeline.create(options);

		JCSMPProperties jcsmpProperties = new JCSMPProperties();
		jcsmpProperties.setProperty(JCSMPProperties.HOST, options.getCip());
		jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, options.getVpn());
		jcsmpProperties.setProperty(JCSMPProperties.USERNAME, options.getCu());
		jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, options.getCp());

		/*
		 * Concept #1: the Beam SDK lets us run the same pipeline with either a bounded or
		 * unbounded input source.
		 */
		PCollection<SolaceTextRecord> input = pipeline
				.apply(SolaceIO.read(jcsmpProperties, queues, SolaceTextRecord.getCoder(), SolaceTextRecord.getMapper())
						.withUseSenderTimestamp(options.getSts())
						.withAdvanceTimeoutInMillis(options.getTimeout()));

		PCollection<SolaceTextRecord> windowedWords = input.apply(
				Window.<SolaceTextRecord>into(FixedWindows.of(Duration.standardSeconds(4))));

		/*
		 * The "sender sequence number" and "message ID" properties are unreliable for detecting message duplicates.
		 * Using it in production can result in message loss.
		 * We're just using it here for convenience.
		 *
		 * We leave it up to you to find a post-processing solution for de-duplication.
		 */
		PCollection<SolaceTextRecord> deduppedInput = windowedWords.apply(Distinct
				.<SolaceTextRecord, byte[]>withRepresentativeValueFn(m ->
						Longs.toByteArray(useSenderMsgId ? m.getSequenceNumber() : m.getMessageId()))
				.withRepresentativeType(TypeDescriptor.of(byte[].class))
		);

		PCollection<String> payloads = deduppedInput.apply(ParDo.of(new DoFn<SolaceTextRecord, String>() {
			@ProcessElement
			public void processElement(@Element SolaceTextRecord record, OutputReceiver<String> receiver) {
				receiver.output(record.getPayload());
			}
		}));

		PCollection<KV<String, Long>> wordCounts = payloads.apply(new CountWords());

		wordCounts.apply(MapElements.via(new WordCountToTextFn()))
				.apply(ParDo.of(new DoFn<String, String>() {
					@ProcessElement
					public void processElement(@Element String e) {
						LOG.info("***" + e);
					}
				}));

		PipelineResult result = pipeline.run();
		try {
			result.waitUntilFinish();
		} catch (Exception exc) {
			result.cancel();
		}
	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		try {
			runWindowedWordCount(options);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
