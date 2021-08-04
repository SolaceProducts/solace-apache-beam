package com.solace.connector.beam.examples;

import com.solace.connector.beam.examples.test.ITEnv;
import com.solace.connector.beam.examples.test.extension.ExecutorServiceExtension;
import com.solace.connector.beam.examples.test.extension.LogCaptorExtension;
import com.solace.connector.beam.examples.test.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(PubSubPlusExtension.class)
public class SolaceRecordTestIT {
	@RegisterExtension
	static ExecutorServiceExtension executorExtension = new ExecutorServiceExtension(() -> Executors.newScheduledThreadPool(5));

	@RegisterExtension
	static LogCaptorExtension logAppenderExtension = new LogCaptorExtension(SolaceRecordTest.class);

	@BeforeAll
	public static void beforeAll() {
		//TODO add support to run these test against Google Dataflow
		assumeTrue(!ITEnv.Test.RUNNER.isPresent() ||
						ITEnv.Test.RUNNER.get().equalsIgnoreCase(DirectRunner.class.getSimpleName()),
				"This test currently only supports the direct runner");
	}

	@Test
	public void testBasic(JCSMPProperties jcsmpProperties, ScheduledExecutorService executorService, BufferedReader logReader)
			throws Exception {
		String vpnName = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		Queue queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(20));
		JCSMPSession jcsmpSession = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);

		try {
			jcsmpSession.connect();
			jcsmpSession.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
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
			args.add(String.format("--cip=%s", jcsmpProperties.getStringProperty(JCSMPProperties.HOST)));
			args.add(String.format("--vpn=%s", vpnName));
			args.add(String.format("--cu=%s", jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME)));
			args.add(String.format("--cp=%s", jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD)));
			args.add(String.format("--sql=%s", queue.getName()));

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
		} finally {
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
			jcsmpSession.closeSession();
		}
	}
}
