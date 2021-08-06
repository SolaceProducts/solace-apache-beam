package com.solace.connector.beam.test.extension;

import com.solace.connector.beam.ITEnv;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.ApiException;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubPlusExtension implements AfterEachCallback, ParameterResolver {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubPlusExtension.class);
	private static final Namespace NAMESPACE = Namespace.create(PubSubPlusExtension.class);
	private final boolean usePubSubPlusTestcontainer;

	public PubSubPlusExtension() {
		usePubSubPlusTestcontainer = ITEnv.Test.USE_TESTCONTAINERS.get("true").equalsIgnoreCase("true") &&
				(!ITEnv.Test.RUNNER.isPresent() ||
						ITEnv.Test.RUNNER.get().equalsIgnoreCase(DirectRunner.class.getSimpleName()));
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		Queue queue = context.getStore(NAMESPACE).get(Queue.class, Queue.class);
		JCSMPSession jcsmpSession = context.getStore(NAMESPACE).get(JCSMPSession.class, JCSMPSession.class);
		if (jcsmpSession != null) {
			try {
				if (queue != null) {
					LOG.info("Deprovisioning queue {}", queue.getName());
					jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
				}
			} finally {
				LOG.info("Closing session");
				jcsmpSession.closeSession();
			}
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		Class<?> paramType = parameterContext.getParameter().getType();
		return JCSMPProperties.class.isAssignableFrom(paramType) ||
				JCSMPSession.class.isAssignableFrom(paramType) ||
				Queue.class.isAssignableFrom(paramType) ||
				SempV2Api.class.isAssignableFrom(paramType);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		Class<?> paramType = parameterContext.getParameter().getType();
		PubSubPlusContainer container;
		if (usePubSubPlusTestcontainer) {
			// Store container in root store so that it's only created once for all test classes.
			container = extensionContext.getRoot().getStore(NAMESPACE).getOrComputeIfAbsent(PubSubPlusContainerResource.class,
					c -> {
						LOG.info("Creating PubSub+ container");
						PubSubPlusContainer newContainer = new PubSubPlusContainer();
						newContainer.start();
						try {
							new SempV2Api(newContainer.getOrigin(PubSubPlusContainer.Port.SEMP),
									newContainer.getAdminUsername(), newContainer.getAdminPassword())
									.config()
									.updateMsgVpnClientUsername("default", "default",
									new ConfigMsgVpnClientUsername().password("default"), null);
						} catch (ApiException e) {
							throw new ParameterResolutionException("Failed to create PubSub+ container", e);
						}
						return new PubSubPlusContainerResource(newContainer);
					}, PubSubPlusContainerResource.class).getContainer();
		} else {
			container = null;
		}

		if (Queue.class.isAssignableFrom(paramType) || JCSMPSession.class.isAssignableFrom(paramType) ||
				JCSMPProperties.class.isAssignableFrom(paramType)) {
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

			if (JCSMPProperties.class.isAssignableFrom(paramType)) {
				return jcsmpProperties;
			}

			JCSMPSession session = extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent(JCSMPSession.class, c -> {
				try {
					LOG.info("Creating JCSMP session");
					JCSMPSession jcsmpSession = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
					jcsmpSession.connect();
					return jcsmpSession;
				} catch (JCSMPException e) {
					throw new ParameterResolutionException("Failed to create JCSMP session", e);
				}
			}, JCSMPSession.class);

			if (JCSMPSession.class.isAssignableFrom(paramType)) {
				return session;
			}

			return extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent(Queue.class, c -> {
				Queue queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(20));
				try {
					LOG.info("Provisioning queue {}", queue.getName());
					session.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
				} catch (JCSMPException e) {
					throw new ParameterResolutionException("Could not create queue", e);
				}
				return queue;
			}, Queue.class);
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

	private static class PubSubPlusContainerResource implements ExtensionContext.Store.CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(PubSubPlusContainerResource.class);
		private final PubSubPlusContainer container;

		private PubSubPlusContainerResource(PubSubPlusContainer container) {
			this.container = container;
		}

		public PubSubPlusContainer getContainer() {
			return container;
		}

		@Override
		public void close() {
			LOG.info("Closing PubSub+ container {}", container.getContainerName());
			container.close();
		}
	}
}
