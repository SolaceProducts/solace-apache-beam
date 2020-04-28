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

import com.solace.connector.beam.SolaceIO;
import com.solace.connector.beam.examples.common.CountWords;
import com.solace.connector.beam.examples.common.StringMessageMapper;
import com.solace.connector.beam.examples.common.WordCountToTextFn;
import com.solace.connector.beam.examples.common.WriteOneFilePerWindow;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.List;

/**
 * An example that counts the number of each word in the received Solace message payloads, outputs the results to
 * the provided output location, and can run over either unbounded or bounded input collections.
 *
 * <p>By default, the examples will run with the {@code DirectRunner}. To run the pipeline on
 * Google Dataflow, specify:
 *
 * <pre>{@code
 * --runner=DataflowRunner
 * }</pre>
 * <p>
 *
 * <p>To execute this pipeline locally, specify a local output file (if using the {@code
 * DirectRunner}) or output prefix on a supported distributed file system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 */
public class WindowedWordCountSolace {

	public interface Options extends PipelineOptions {
		@Description("IP and port of the client appliance. (e.g. -cip=192.168.160.101)")
		String getCip();

		void setCip(String value);

		@Description("Client username")
		String getCu();

		void setCu(String value);

		@Description("VPN name")
		String getVpn();

		void setVpn(String value);

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

		@Description("The timeout in milliseconds while try to receive a messages from Solace broker")
		@Default.Integer(100)
		int getTimeout();

		void setTimeout(int timeoutInMillis);

		/**
		 * Set this required option to specify where to write the output.
		 */
		@Description("Path of the file to write to")
		@Validation.Required
		String getOutput();

		void setOutput(String value);
	}

	static void runWindowedWordCount(Options options) throws Exception {

		List<String> queues = Arrays.asList(options.getSql().split(","));

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
		PCollection<String> input =
				pipeline
						/*
						 * Read from the Solace PubSub+ broker.
						 * Reading the message as a String isn't recommended since post-deduplication is required.
						 */
						.apply(SolaceIO.read(jcsmpProperties, queues, StringUtf8Coder.of(), new StringMessageMapper())
								.withUseSenderTimestamp(options.getSts())
								.withAdvanceTimeoutInMillis(options.getTimeout()));

		/*
		 * Concept #3: Window into fixed windows. The fixed window size for this example defaults to 10 seconds.
		 */
		PCollection<String> windowedWords =
				input.apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))));

		/*
		 * Concept #4: Re-use our existing CountWords transform that does not have knowledge of
		 * windows over a PCollection containing windowed values.
		 */
		PCollection<KV<String, Long>> wordCounts = windowedWords.apply(new CountWords());

		/*
		 * Concept #5: Format the results and write to a sharded file partitioned by window, using a
		 * simple ParDo operation. Because there may be failures followed by retries, the
		 * writes must be idempotent, but the details of writing to files is elided here.
		 */
		final String output = options.getOutput();
		wordCounts
				.apply(MapElements.via(new WordCountToTextFn()))
				.apply(new WriteOneFilePerWindow(output, 1));

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
