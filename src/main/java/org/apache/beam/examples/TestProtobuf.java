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

import com.solacesystems.jcsmp.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class TestProtobuf {
    private static final Logger LOG = LoggerFactory.getLogger(TestProtobuf.class);

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

        @Description("queue for subscribing/publishing")
        String getQueue();
        void setQueue(String value);
    }

    public static class ByteArrayMapper implements SolaceIO.MessageMapper<byte[]> {
        private static final long serialVersionUID = 42L;

        @Override
        public byte[] mapMessage(BytesXMLMessage message) throws Exception {
            if (message instanceof BytesMessage) {
                BytesMessage bm = (BytesMessage)message;
                return bm.getData();
            } else throw new RuntimeException("expected BytesMessage");
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        try {
            JCSMPProperties jcsmpProperties = new JCSMPProperties();

            String[] parts = options.getCu().split("@");

            jcsmpProperties.setProperty(JCSMPProperties.HOST,
                    options.getCip());
            jcsmpProperties.setProperty(JCSMPProperties.USERNAME, parts[0]);
            jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, parts[1]);
            jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, options.getCp());

            JCSMPSession session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);

            session.connect();

            Queue queue = JCSMPFactory.onlyInstance().createQueue(options.getQueue());

            EndpointProperties endpointProps = new EndpointProperties();
            endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

            session.provision(queue,
                    endpointProps,
                    JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

            XMLMessageProducer producer =
                    session.getMessageProducer(
                            new JCSMPStreamingPublishEventHandler() {
                                @Override
                                public void responseReceived(String s) {

                                }

                                @Override
                                public void handleError(String s, JCSMPException e, long l) {

                                }
                            });

            BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            byte[] test = TestOuterClass.Test.newBuilder().setTest("test").build().toByteArray();
            LOG.info(TestOuterClass.Test.parseFrom(test).toString());

            msg.setData(test);
            for (int i=0; i < 1000; i++) {
                producer.send(msg, queue);
            }
            LOG.info("published messages");
        } catch (Exception e) {
            System.out.println(e);
        }



        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(SolaceIO.<byte[]>readMessage()
                        .withConnectionConfiguration(SolaceIO.ConnectionConfiguration.create(options.getCip(), Arrays.asList(options.getQueue()))
                                .withUsername(options.getCu())
                                .withPassword(options.getCp())
                        )
                        .withCoder(ByteArrayCoder.of())
                        .withMessageMapper(new ByteArrayMapper())
                )
                .apply(MapElements.via(new InferableFunction<byte[], TestOuterClass.Test>() {
                    @Override
                    public TestOuterClass.Test apply(byte[] input) throws Exception {
                        return TestOuterClass.Test.parseFrom(input);
                    }
                }))
        .apply(MapElements.into(TypeDescriptors.nulls()).via((TestOuterClass.Test x) -> {LOG.info(x.toString()); return (Void) null;}));

        pipeline.run();
    }
}
