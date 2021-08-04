package com.solace.connector.beam.examples.test.extension;

import com.solace.connector.beam.examples.test.ITEnv;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.ApiException;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.runners.direct.DirectRunner;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class PubSubPlusExtension implements AfterAllCallback, ParameterResolver {
	private static final Namespace NAMESPACE = Namespace.create(PubSubPlusExtension.class);
	private final boolean usePubSubPlusTestcontainer;

	public PubSubPlusExtension() {
		usePubSubPlusTestcontainer = ITEnv.Test.USE_TESTCONTAINERS.get("true").equalsIgnoreCase("true") &&
				(!ITEnv.Test.RUNNER.isPresent() ||
						ITEnv.Test.RUNNER.get().equalsIgnoreCase(DirectRunner.class.getSimpleName()));
	}

	@Override
	public void afterAll(ExtensionContext extensionContext) {
		PubSubPlusContainer container = extensionContext.getStore(NAMESPACE).get(PubSubPlusContainer.class, PubSubPlusContainer.class);
		if (container != null) {
			container.close();
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		Class<?> paramType = parameterContext.getParameter().getType();
		return JCSMPProperties.class.isAssignableFrom(paramType) || SempV2Api.class.isAssignableFrom(paramType);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		Class<?> paramType = parameterContext.getParameter().getType();
		PubSubPlusContainer container;
		if (usePubSubPlusTestcontainer) {
			container = extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent(PubSubPlusContainer.class,
					c -> {
						PubSubPlusContainer newContainer = new PubSubPlusContainer();
						newContainer.start();
						try {
							new SempV2Api(newContainer.getOrigin(PubSubPlusContainer.Port.SEMP),
									newContainer.getAdminUsername(), newContainer.getAdminPassword())
									.config()
									.updateMsgVpnClientUsername("default", "default",
									new ConfigMsgVpnClientUsername().password("default"), null);
						} catch (ApiException e) {
							throw new RuntimeException(e);
						}
						return newContainer;
					}, PubSubPlusContainer.class);
		} else {
			container = null;
		}

		if (JCSMPProperties.class.isAssignableFrom(paramType)) {
			JCSMPProperties jcsmpProperties = new JCSMPProperties();
			if (container != null) {
				jcsmpProperties.setProperty(JCSMPProperties.HOST, container.getOrigin(PubSubPlusContainer.Port.SMF));
				jcsmpProperties.setProperty(JCSMPProperties.USERNAME, "default");
				jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, "default");
				jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, "default");
			} else {
				jcsmpProperties.setProperty(JCSMPProperties.HOST, ITEnv.Solace.HOST.get("tcp://localhost:55555"));
				jcsmpProperties.setProperty(JCSMPProperties.USERNAME, ITEnv.Solace.USERNAME.get("default"));
				jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, ITEnv.Solace.PASSWORD.get("default"));
				jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, ITEnv.Solace.VPN.get("default"));
			}
			return jcsmpProperties;
		} else if (SempV2Api.class.isAssignableFrom(paramType)) {
			if (container != null) {
				return new SempV2Api(container.getOrigin(PubSubPlusContainer.Port.SEMP), container.getAdminUsername(),
						container.getAdminPassword());
			} else {
				return new SempV2Api(ITEnv.Solace.MGMT_HOST.get("http://localhost:8080"),
						ITEnv.Solace.MGMT_USERNAME.get("admin"), ITEnv.Solace.MGMT_PASSWORD.get("admin"));
			}
		} else {
			throw new IllegalArgumentException(String.format("Parameter type %s is not supported", paramType));
		}
	}
}
