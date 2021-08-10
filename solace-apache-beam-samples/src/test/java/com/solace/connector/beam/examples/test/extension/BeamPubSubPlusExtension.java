package com.solace.connector.beam.examples.test.extension;

import com.solace.connector.beam.examples.test.SolaceTestOptions;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.ApiException;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.extension.ParameterResolutionException;

public class BeamPubSubPlusExtension extends PubSubPlusExtension {
	private static final String DEFAULT_CLIENT_PASSWORD = "default";

	public BeamPubSubPlusExtension() {
		super(() -> {
			SolaceTestOptions options = getTestOptions();
			return options.getUseTestcontainers() && options.getRunner().equals(DirectRunner.class);
		});
	}

	@Override
	protected void containerStartCallback(PubSubPlusContainer container) {
		try {
			new SempV2Api(container.getOrigin(PubSubPlusContainer.Port.SEMP), container.getAdminUsername(),
					container.getAdminPassword())
					.config()
					.updateMsgVpnClientUsername("default", "default",
							new ConfigMsgVpnClientUsername().password(DEFAULT_CLIENT_PASSWORD), null);
		} catch (ApiException e) {
			throw new ParameterResolutionException("Failed to create PubSub+ container", e);
		}
	}

	@Override
	protected JCSMPProperties createContainerJcsmpProperties(PubSubPlusContainer container) {
		JCSMPProperties jcsmpProperties = super.createContainerJcsmpProperties(container);
		jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, DEFAULT_CLIENT_PASSWORD);
		return jcsmpProperties;
	}

	@Override
	protected JCSMPProperties createDefaultJcsmpProperties() {
		SolaceTestOptions options = getTestOptions();
		JCSMPProperties jcsmpProperties = new JCSMPProperties();
		jcsmpProperties.setProperty(JCSMPProperties.HOST, options.getPspHost());
		jcsmpProperties.setProperty(JCSMPProperties.USERNAME, options.getPspUsername());
		jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, options.getPspPassword());
		jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, options.getPspVpnName());
		return jcsmpProperties;
	}

	@Override
	protected SempV2Api createDefaultSempV2Api() {
		SolaceTestOptions options = getTestOptions();
		return new SempV2Api(options.getPspMgmtHost(), options.getPspMgmtUsername(), options.getPspMgmtPassword());
	}

	private static SolaceTestOptions getTestOptions() {
		PipelineOptionsFactory.register(SolaceTestOptions.class);
		return TestPipeline.testingPipelineOptions().as(SolaceTestOptions.class);
	}
}
