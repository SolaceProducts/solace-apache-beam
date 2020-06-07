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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.solace.connector.beam.SolaceIO;
import com.solace.connector.beam.examples.common.SolaceTextRecord;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An example that takes binds to a Solace queue, consumes messages, and then writes them to BigQuery.
 * 
 * The code is written to handle payload of this format:
 * [{"date":"2020-06-06","sym":"XOM","time":"22:58","lowAskSize":20,"highAskSize":790,"lowBidPrice":43.13057,
 * "highBidPrice":44.95833,"lowBidSize":60,"highBidSize":770,"lowTradePrice":43.51274,"highTradePrice":45.41246,
 * "lowTradeSize":0,"highTradeSize":480,"lowAskPrice":43.67592,"highAskPrice":45.86658,"vwap":238.0331}]
 * 
 * You will need to make sure there is a BigQuery table with appropriate schema already created.
 * 
 * <p>By default, the examples will run with the {@code DirectRunner}. To run the pipeline on
 * Google Dataflow, specify:
 *
 * <pre>{@code
 * --runner=DataflowRunner
 * }</pre>
 * <p>
 */
public class SolaceBigQuery {
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

	}


	private static void kdbBigQuery(Options options) throws Exception {

		List<String> queues = Arrays.asList(options.getSql().split(","));
		boolean useSenderMsgId = options.getSmi();

		TableSchema tableSchema = new TableSchema()
						.setFields(ImmutableList.of(
								new TableFieldSchema().setName("date").setType("DATE").setMode("NULLABLE"),
								new TableFieldSchema().setName("sym").setType("STRING").setMode("NULLABLE"),
								new TableFieldSchema().setName("time").setType("TIME").setMode("NULLABLE"),
								new TableFieldSchema().setName("lowAskSize").setType("INTEGER").setMode("NULLABLE"),
								new TableFieldSchema().setName("highAskSize").setType("INTEGER").setMode("NULLABLE"),
								new TableFieldSchema().setName("lowBidPrice").setType("FLOAT").setMode("NULLABLE"),
								new TableFieldSchema().setName("highBidPrice").setType("FLOAT").setMode("NULLABLE"),
								new TableFieldSchema().setName("lowBidSize").setType("INTEGER").setMode("NULLABLE"),
								new TableFieldSchema().setName("highBidSize").setType("INTEGER").setMode("NULLABLE"),
								new TableFieldSchema().setName("lowTradePrice").setType("FLOAT").setMode("NULLABLE"),
								new TableFieldSchema().setName("highTradePrice").setType("FLOAT").setMode("NULLABLE"),
								new TableFieldSchema().setName("lowTradeSize").setType("INTEGER").setMode("NULLABLE"),
								new TableFieldSchema().setName("highTradeSize").setType("INTEGER").setMode("NULLABLE"),
								new TableFieldSchema().setName("lowAskPrice").setType("FLOAT").setMode("NULLABLE"),
								new TableFieldSchema().setName("highAskPrice").setType("FLOAT").setMode("NULLABLE"),
								new TableFieldSchema().setName("vwap").setType("FLOAT").setMode("NULLABLE")));

		// Insert information about your table (projectId, datasetId, and tableId)
		TableReference tableSpec =
				new TableReference()
						.setProjectId("<project_id>")
						.setDatasetId("<dataset_id>")
						.setTableId("<table_id>");

		Pipeline pipeline = Pipeline.create(options);

		JCSMPProperties jcsmpProperties = new JCSMPProperties();
		jcsmpProperties.setProperty(JCSMPProperties.HOST, options.getCip());
		jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, options.getVpn());
		jcsmpProperties.setProperty(JCSMPProperties.USERNAME, options.getCu());
		jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, options.getCp());

		/* The pipeline consists of three components:
		* 1. Reading message from Solace queue
		* 2. Transforming the message and mapping it to a BigQuery Table Row
		* 3. Writing the row to BigQuery
		*/ 
		WriteResult input = pipeline
				.apply(SolaceIO.read(jcsmpProperties, queues, SolaceTextRecord.getCoder(), SolaceTextRecord.getMapper())
						.withUseSenderTimestamp(options.getSts())
						.withAdvanceTimeoutInMillis(options.getTimeout()))

				.apply("MapToTableRow", ParDo.of(new DoFn<SolaceTextRecord, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c) {

						Map<String, String> parsedMap = new HashMap<String, String>();
						
						// Clean up the payload so we can easily map the values into a HashMap
						String[] pairs = c.element().getPayload().replace("[", "")
								.replace("]", "")
								.replace("{", "")
								.replace("}", "")
								.replace("\"", "").split(",");

						for (int i=0;i<pairs.length;i++) {
							String pair = pairs[i];
							String[] keyValue = pair.split(":");
							parsedMap.put(keyValue[0], keyValue[1]);
						}

						TableRow row = new TableRow();
						row.set("date", parsedMap.get("date"));
						row.set("sym", parsedMap.get("sym"));
						row.set("time", parsedMap.get("time"));
						row.set("lowAskSize", Integer.parseInt(parsedMap.get("lowAskSize")));
						row.set("highAskSize", Integer.parseInt(parsedMap.get("highAskSize")));
						row.set("lowBidPrice", Float.parseFloat(parsedMap.get("lowBidPrice")));
						row.set("highBidPrice", Float.parseFloat(parsedMap.get("highBidPrice")));
						row.set("lowBidSize", Integer.parseInt(parsedMap.get("lowBidSize")));
						row.set("highBidSize", Integer.parseInt(parsedMap.get("highBidSize")));
						row.set("lowTradePrice", Float.parseFloat(parsedMap.get("lowTradePrice")));
						row.set("highTradePrice", Float.parseFloat(parsedMap.get("highTradePrice")));
						row.set("lowTradeSize", Integer.parseInt(parsedMap.get("lowTradeSize")));
						row.set("highTradeSize", Integer.parseInt(parsedMap.get("highTradeSize")));
						row.set("lowAskPrice", Float.parseFloat(parsedMap.get("lowAskPrice")));
						row.set("highAskPrice", Float.parseFloat(parsedMap.get("highAskPrice")));
						row.set("vwap", Float.parseFloat(parsedMap.get("vwap")));
						c.output(row);
					}
				}))

				.apply("CommitToBQTable", BigQueryIO.writeTableRows()
				.to(tableSpec)
				.withSchema(tableSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

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
			SolaceBigQuery(options);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}