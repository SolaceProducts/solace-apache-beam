package com.solace.connector.beam.examples;

import com.solace.connector.beam.examples.test.ITEnv;
import com.solace.connector.beam.examples.test.extension.ExecutorServiceExtension;
import com.solace.connector.beam.examples.test.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.connector.beam.examples.test.extension.LogCaptorExtension;
import com.solace.connector.beam.examples.test.extension.LogCaptorExtension.LogCaptor;
import com.solace.connector.beam.examples.test.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(LogCaptorExtension.class)
@ExtendWith(PubSubPlusExtension.class)
public class SolaceRecordTestIT {
	@BeforeAll
	public static void beforeAll() {
		//TODO add support to run these test against Google Dataflow
		assumeTrue(!ITEnv.Test.RUNNER.isPresent() ||
						ITEnv.Test.RUNNER.get().equalsIgnoreCase(DirectRunner.class.getSimpleName()),
				"This test currently only supports the direct runner");
	}

	@Test
	public void testBasic(JCSMPSession jcsmpSession, Queue queue,
						  @ExecSvc(poolSize = 2, scheduled = true) ScheduledExecutorService executorService,
						  @LogCaptor(SolaceRecordTest.class) BufferedReader logReader) throws Exception {
		final XMLMessageProducer producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
			@Override
			public void responseReceivedEx(Object o) {}

			@Override
			public void handleErrorEx(Object o, JCSMPException e, long l) {}
		});

		executorService.scheduleAtFixedRate(() -> {
			TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			message.setText("Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
			try {
				producer.send(message, queue);
			} catch (JCSMPException e) {
				throw new RuntimeException(e);
			}
		}, 0, 100, TimeUnit.MILLISECONDS);

		List<String> args = new ArrayList<>();
		args.add(String.format("--cip=%s", jcsmpSession.getProperty(JCSMPProperties.HOST)));
		args.add(String.format("--vpn=%s", jcsmpSession.getProperty(JCSMPProperties.VPN_NAME)));
		args.add(String.format("--cu=%s", jcsmpSession.getProperty(JCSMPProperties.USERNAME)));
		args.add(String.format("--cp=%s", jcsmpSession.getProperty(JCSMPProperties.PASSWORD)));
		args.add(String.format("--sql=%s", queue.getName()));
		Optional.ofNullable(TestPipeline.testingPipelineOptions().as(ExperimentalOptions.class))
				.map(options -> PipelineOptionsValidator.validate(ExperimentalOptions.class, options))
				.map(ExperimentalOptions::getExperiments)
				.map(experiments -> String.join(",", experiments))
				.map(experiments -> String.format("--experiments=%s", experiments))
				.ifPresent(args::add);

		executorService.submit(() -> SolaceRecordTest.main(args.toArray(new String[0])));
		Assertions.assertTimeoutPreemptively(Duration.ofMinutes(5), () -> {
			while (true) {
				String line;
				if ((line = logReader.readLine()) != null) {
					if (line.contains("Lorem")) {
						return null;
					} else {
						Thread.sleep(Duration.ofSeconds(1).toMillis());
					}
				}
			}
		});
	}
}
