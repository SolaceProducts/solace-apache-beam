package com.solace.connector.beam.test.pubsub;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PublisherEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {
	private static final Logger LOG = LoggerFactory.getLogger(PublisherEventHandler.class);

	@Override
	public void responseReceivedEx(Object key) {
		LOG.debug("Producer received response for msg: " + key);
		if (key instanceof CallbackCorrelationKey) {
			((CallbackCorrelationKey) key).runOnSuccess();
		}
	}

	@Override
	public void handleErrorEx(Object key, JCSMPException e, long timestamp) {
		LOG.warn("Producer received error for msg: " + key + " - " + timestamp, e);
		if (key instanceof CallbackCorrelationKey) {
			((CallbackCorrelationKey) key).runOnFailure(e, timestamp);
		}
	}
}
