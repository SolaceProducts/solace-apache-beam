package com.solace.connector.beam.test.pubsub;

import com.solacesystems.jcsmp.JCSMPException;

import java.util.function.BiConsumer;

public class CallbackCorrelationKey {
	private Runnable onSuccess;
	private BiConsumer<JCSMPException, Long> onFailure;

	void runOnSuccess() {
		if (onSuccess != null) {
			onSuccess.run();
		}
	}

	void runOnFailure(JCSMPException e, Long timestamp) {
		if (onFailure != null) {
			onFailure.accept(e, timestamp);
		}
	}

	public void setOnSuccess(Runnable onSuccess) {
		this.onSuccess = onSuccess;
	}

	public void setOnFailure(BiConsumer<JCSMPException, Long> onFailure) {
		this.onFailure = onFailure;
	}
}
