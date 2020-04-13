package com.solace.apache.beam.test.util;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class GoogleStorageUtil extends GoogleUtilBase {
	private static Map<String, Storage> storageByProjectId = new HashMap<>();

	private GoogleStorageUtil() {}

	public static BlobId createBlobId(String url) {
		Pattern pattern = Pattern.compile("^gs://(.*?)/(.*?)$");
		Matcher matcher = pattern.matcher(url);
		if (matcher.find() && !matcher.group(1).isEmpty() && !matcher.group(2).isEmpty()) {
			return BlobId.of(matcher.group(1), matcher.group(2));
		} else {
			throw new IllegalArgumentException(String.format("URL %s does not match %s", url, pattern));
		}
	}

	public static Iterable<Blob> getBlobs(String projectId, String bucketName, String objectPrefix) throws IOException {
		return getService(projectId)
				.list(bucketName,
						Storage.BlobListOption.prefix(objectPrefix),
						Storage.BlobListOption.currentDirectory())
				.iterateAll();
	}

	private static Storage getService(String projectId) throws IOException {
		if (!storageByProjectId.containsKey(projectId)) {
			storageByProjectId.put(projectId, StorageOptions.newBuilder()
					.setProjectId(projectId).setCredentials(getCredentials())
					.build()
					.getService());
		}

		return storageByProjectId.get(projectId);
	}
}
