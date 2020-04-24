package com.solace.connector.beam.test.util;

import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.util.Collections;

abstract class GoogleUtilBase {
	static final String GOOGLE_AUTH_CLOUD_URL = "https://www.googleapis.com/auth/cloud-platform";

	static GoogleCredentials getCredentials() throws IOException {
		GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

		if (credentials.createScopedRequired()) {
			credentials = credentials.createScoped(Collections.singletonList(GOOGLE_AUTH_CLOUD_URL));
		}

		return credentials;
	}
}
