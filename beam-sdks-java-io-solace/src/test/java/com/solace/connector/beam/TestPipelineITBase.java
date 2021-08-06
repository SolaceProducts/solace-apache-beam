package com.solace.connector.beam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.connector.beam.test.pubsub.PublisherEventHandler;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.junit.Assert.fail;

public abstract class TestPipelineITBase {
	private static final Logger LOG = LoggerFactory.getLogger(SolaceIOIT.class);

	transient TestPipeline testPipeline;
	PipelineOptions pipelineOptions;
	JCSMPProperties testJcsmpProperties;
	JCSMPSession jcsmpSession;
	XMLMessageProducer producer;

	/**
	 * @deprecated use {@link #sempV2Api}
	 */
	@Deprecated
	SempOperationUtils sempOps;

	public static SempV2Api sempV2Api;
	static PipelineOptions sharedPipelineOptions;
	private static PubSubPlusContainer pubSubPlusContainer;
	private static JCSMPProperties detectedJcsmpProperties;
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
	public static void initTestProperties() throws Exception {
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
		} else if (ITEnv.Test.USE_TESTCONTAINERS.get("true").equalsIgnoreCase("true")) {
			// Testcontainers only makes sense to be used with the direct runner
			pubSubPlusContainer = new PubSubPlusContainer();
			pubSubPlusContainer.start();
		}

		LOG.info("Initializing PubSub+ broker credentials");
		SolaceIOTestPipelineOptions solaceOps = sharedPipelineOptions.as(SolaceIOTestPipelineOptions.class);
		PipelineOptionsValidator.validate(SolaceIOTestPipelineOptions.class, solaceOps);

		detectedJcsmpProperties = new JCSMPProperties();

		if (pubSubPlusContainer != null) {
			detectedJcsmpProperties.setProperty(JCSMPProperties.HOST, pubSubPlusContainer.getOrigin(PubSubPlusContainer.Port.SMF));
			detectedJcsmpProperties.setProperty(JCSMPProperties.USERNAME, "default");
			detectedJcsmpProperties.setProperty(JCSMPProperties.PASSWORD, "default");
			detectedJcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, "default");

			sempV2Api = new SempV2Api(pubSubPlusContainer.getOrigin(PubSubPlusContainer.Port.SEMP),
					pubSubPlusContainer.getAdminUsername(), pubSubPlusContainer.getAdminPassword());

			sempV2Api.config()
					.updateMsgVpnClientUsername("default", "default",
					new ConfigMsgVpnClientUsername().password("default"), null);
		} else {
			detectedJcsmpProperties.setProperty(JCSMPProperties.HOST, ITEnv.Solace.HOST.get(solaceOps.getSolaceHost()));
			detectedJcsmpProperties.setProperty(JCSMPProperties.USERNAME, ITEnv.Solace.USERNAME.get(solaceOps.getSolaceUsername()));
			detectedJcsmpProperties.setProperty(JCSMPProperties.PASSWORD, ITEnv.Solace.PASSWORD.get(solaceOps.getSolacePassword()));
			detectedJcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, ITEnv.Solace.VPN.get(solaceOps.getSolaceVpnName()));

			sempV2Api = new SempV2Api(ITEnv.Solace.MGMT_HOST.get(solaceOps.getSolaceMgmtHost()),
					ITEnv.Solace.MGMT_USERNAME.get(solaceOps.getSolaceMgmtUsername()),
					ITEnv.Solace.MGMT_PASSWORD.get(solaceOps.getSolaceMgmtPassword()));
		}
	}

	@Before
	public void setupConnection() throws Exception {
		testPipeline.getOptions().setJobName(generateJobName(pipelineOptions));

		testJcsmpProperties = (JCSMPProperties) detectedJcsmpProperties.clone();
		testJcsmpProperties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 255); // max it out for faster publishes

		LOG.info(String.format("Creating JCSMP Session for %s", testJcsmpProperties.getStringProperty(JCSMPProperties.HOST)));
		jcsmpSession = JCSMPFactory.onlyInstance().createSession(testJcsmpProperties);

		LOG.info(String.format("Creating XMLMessageProducer for %s", testJcsmpProperties.getStringProperty(JCSMPProperties.HOST)));
		producer = jcsmpSession.getMessageProducer(new PublisherEventHandler());

		sempOps = new SempOperationUtils(sempV2Api, jcsmpSession, false, true);
		sempOps.start();
	}

	@AfterClass
	public static void teardownGlobalResources() {
		if (pubSubPlusContainer != null) {
			pubSubPlusContainer.close();
		}
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
}
