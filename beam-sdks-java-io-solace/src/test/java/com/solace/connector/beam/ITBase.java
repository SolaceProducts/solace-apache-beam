package com.solace.connector.beam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.testing.TestPipeline;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.junit.Assert.fail;

public abstract class ITBase {
	private static final Logger LOG = LoggerFactory.getLogger(SolaceIOIT.class);

	transient TestPipeline testPipeline;
	PipelineOptions pipelineOptions;
	JCSMPProperties testJcsmpProperties;
	JCSMPSession jcsmpSession;
	XMLMessageProducer producer;
	SempOperationUtils sempOps;

	static PipelineOptions sharedPipelineOptions;
	private static JCSMPProperties detectedJcsmpProperties;
	private static String mgmtHost;
	private static String mgmtUsername;
	private static String mgmtPassword;
	private static final String DATAFLOW_REGION_US_CENTRAL1 = "us-central1";
	private static final ObjectMapper MAPPER = new ObjectMapper();

	@Rule
	public TestPipeline getTestPipeline() {
		LOG.info(String.format("Generating new %s", TestPipeline.class.getSimpleName()));
		pipelineOptions = MAPPER.convertValue(MAPPER.valueToTree(sharedPipelineOptions), PipelineOptions.class);
		testPipeline = TestPipeline.fromOptions(pipelineOptions);
		return testPipeline;
	}

	@BeforeClass
	public static void initTestProperties() {
		PipelineOptionsFactory.register(SolaceIOTestPipelineOptions.class);
		sharedPipelineOptions = TestPipeline.testingPipelineOptions();

		if (ITEnv.Test.RUNNER.isPresent()) {
			String runnerName = ITEnv.Test.RUNNER.get();
			if (runnerName.equals(TestDataflowRunner.class.getSimpleName())) {
				sharedPipelineOptions.setRunner(TestDataflowRunner.class);
			} else if (runnerName.equals(DirectRunner.class.getSimpleName())) {
				sharedPipelineOptions.setRunner(DirectRunner.class);
			} else {
				fail(String.format("Runner %s is not supported. Please provide one of: [%s]",
						sharedPipelineOptions.getRunner().getSimpleName(),
						String.join(", ",
								TestDataflowRunner.class.getSimpleName(),
								DirectRunner.class.getSimpleName())));
			}
		}

		if (sharedPipelineOptions.getRunner().equals(TestDataflowRunner.class)) {
			TestDataflowPipelineOptions dataflowOps = sharedPipelineOptions.as(TestDataflowPipelineOptions.class);

			LOG.info(String.format("Extracting env to pipeline options for %s", TestDataflowRunner.class.getSimpleName()));
			dataflowOps.setProject(ITEnv.Dataflow.PROJECT.get(dataflowOps.getProject()));
			dataflowOps.setTempRoot(ITEnv.Dataflow.TMP_ROOT.get(dataflowOps.getTempRoot()));
			dataflowOps.setGcpTempLocation(dataflowOps.getTempRoot());

			LOG.info(String.format("Setting fixed pipeline options for %s", TestDataflowRunner.class.getSimpleName()));
			dataflowOps.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);
			dataflowOps.setNumWorkers(2);
			dataflowOps.setMaxNumWorkers(5);
			dataflowOps.setRegion(DATAFLOW_REGION_US_CENTRAL1);
			dataflowOps.setWorkerMachineType("n1-standard-1");
			PipelineOptionsValidator.validate(TestDataflowPipelineOptions.class, dataflowOps);
		} else if (!sharedPipelineOptions.getRunner().equals(DirectRunner.class)) {
			fail(String.format("Runner %s is not supported. Please provide one of: [%s]",
					sharedPipelineOptions.getRunner().getSimpleName(),
					String.join(", ",
							TestDataflowRunner.class.getSimpleName(),
							DirectRunner.class.getSimpleName())));
		}

		LOG.info("Initializing PubSub+ broker credentials");
		SolaceIOTestPipelineOptions solaceOps = sharedPipelineOptions.as(SolaceIOTestPipelineOptions.class);
		PipelineOptionsValidator.validate(SolaceIOTestPipelineOptions.class, solaceOps);

		String solaceHostName = ITEnv.Solace.HOST.get(solaceOps.getSolaceHostName());

		detectedJcsmpProperties = new JCSMPProperties();
		detectedJcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, ITEnv.Solace.VPN.get(solaceOps.getSolaceVpnName()));
		detectedJcsmpProperties.setProperty(JCSMPProperties.HOST, String.format("tcp://%s:%s", solaceHostName,
				ITEnv.Solace.SMF_PORT.get(String.valueOf(solaceOps.getSolaceSmfPort()))));
		detectedJcsmpProperties.setProperty(JCSMPProperties.USERNAME, ITEnv.Solace.USERNAME.get(solaceOps.getSolaceUsername()));
		detectedJcsmpProperties.setProperty(JCSMPProperties.PASSWORD, ITEnv.Solace.PASSWORD.get(solaceOps.getSolacePassword()));

		mgmtHost = String.format("https://%s:%s", solaceHostName,
				ITEnv.Solace.MGMT_PORT.get(String.valueOf(solaceOps.getSolaceMgmtPort())));
		mgmtUsername = ITEnv.Solace.MGMT_USERNAME.get(solaceOps.getSolaceMgmtUsername());
		mgmtPassword = ITEnv.Solace.MGMT_PASSWORD.get(solaceOps.getSolaceMgmtPassword());
	}

	@Before
	public void setupConnection() throws Exception {
		testPipeline.getOptions().setJobName(generateJobName(pipelineOptions));

		testJcsmpProperties = (JCSMPProperties) detectedJcsmpProperties.clone();

		LOG.info(String.format("Creating JCSMP Session for %s", testJcsmpProperties.getStringProperty(JCSMPProperties.HOST)));
		jcsmpSession = JCSMPFactory.onlyInstance().createSession(testJcsmpProperties);

		LOG.info(String.format("Creating XMLMessageProducer for %s", testJcsmpProperties.getStringProperty(JCSMPProperties.HOST)));
		producer = jcsmpSession.getMessageProducer(createPublisherEventHandler());

		sempOps = new SempOperationUtils(mgmtHost, mgmtUsername, mgmtPassword, jcsmpSession, false, true);
		sempOps.start();
	}

	@After
	public void teardownConnection() {
		if (jcsmpSession != null && !jcsmpSession.isClosed()) {
			if (sempOps != null) {
				sempOps.close();
			}

			if (producer != null) {
				producer.close();
			}

			LOG.info("Closing JCSMP Session");
			jcsmpSession.closeSession();
		}
	}

	private static String generateJobName(PipelineOptions pipelineOptions) {
		String appName = pipelineOptions.as(ApplicationNameOptions.class).getAppName();
		String normalizedAppName = appName != null && !appName.isEmpty() ?
				appName.replaceAll("[^\\w]", "-").replaceFirst("^[^a-zA-Z]", "a") :
				UUID.randomUUID().toString().replaceAll("-", "");
		String username = System.getProperty("user.name", UUID.randomUUID().toString().replaceAll("-", ""));
		String timestamp = DateTimeFormat.forPattern("yyyyMMdd-HHmmss").print(DateTimeUtils.currentTimeMillis());
		String random = UUID.randomUUID().toString().replaceAll("-", "");
		return String.format("%s-%s-%s-%s",normalizedAppName, username, timestamp, random).toLowerCase();
	}

	static JCSMPStreamingPublishEventHandler createPublisherEventHandler() {
		return new JCSMPStreamingPublishEventHandler() {
			@Override
			public void responseReceived(String messageID) {
				LOG.debug("Producer received response for msg: " + messageID);
			}

			@Override
			public void handleError(String messageID, JCSMPException e, long timestamp) {
				LOG.warn("Producer received error for msg: " + messageID + " - " + timestamp, e);
			}
		};
	}
}
