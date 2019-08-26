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
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.SolaceTextRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class SolaceReproduceStuckConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(SolaceReproduceStuckConsumer.class);

    public interface Options
            extends PipelineOptions {
              @Description("IP and port of the client appliance. (e.g. -cip=192.168.160.101)")
              String getCip();
              void setCip(String value);
          
              @Description("Client username and optionally VPN name.")
              String getCu();
              void setCu(String value);

              @Description("Client password (default '')")
              @Default.String("")
              String getCp();
              void setCp(String value);

              @Description("List of queues for subscribing")
              String getSql();
              void setSql(String value);
            }

  static void run(Options options) throws Exception {

    List<String> queues =Arrays.asList(options.getSql().split(","));

    Pipeline pipeline = Pipeline.create(options);

    PCollection<SolaceTextRecord> input =
        pipeline
          .apply(SolaceIO.<SolaceTextRecord>readMessage()
                  .withConnectionConfiguration(
                          SolaceIO.ConnectionConfiguration.create(options.getCip(), queues)
                                  .withUsername(options.getCu())
                                  .withPassword(options.getCp())
                                  .withAutoAck(false))
                  .withCoder(SolaceTextRecord.getCoder())
                  .withMessageMapper(SolaceTextRecord.getMapper())
          )
    .apply(MapElements.via(new SimpleFunction<SolaceTextRecord, SolaceTextRecord>() {
        @Override
        public SolaceTextRecord apply(SolaceTextRecord input) {
            spin(100);
            return input;
        }
    }));

      PipelineResult result = pipeline.run();
    try {
      result.waitUntilFinish();
    } catch (Exception exc) {
      result.cancel();
    }
  }

    private static void spin(int milliseconds) {
        long sleepTime = milliseconds*1000000L; // convert to nanoseconds
        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) < sleepTime) {}
    }

  public static void main(String[] args) throws Exception {
//    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
      Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    try {
      run(options);
    } catch (Exception e){
      e.printStackTrace();
    }
  }
}
