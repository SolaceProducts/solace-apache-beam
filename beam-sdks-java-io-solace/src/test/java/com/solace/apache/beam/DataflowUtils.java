package com.solace.apache.beam;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.ListJobMessagesResponse;
import com.google.api.services.dataflow.model.ListJobsResponse;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public class DataflowUtils {
	private static final Logger LOG = LoggerFactory.getLogger(DataflowUtils.class);

	private DataflowUtils() {
	}

	public static Job getJob(DataflowPipelineOptions options)
			throws IOException, GeneralSecurityException, InterruptedException {

		String jobName = options.getJobName();
		String projectId = options.getProject();
		String region = options.getRegion();

		for (int attempt = 0; attempt < 60; attempt++) {
			LOG.info(String.format("Getting Google Dataflow info for job name %s of project ID %s in region %s",
					jobName, projectId, region));
			String token;
			do {
				ListJobsResponse response = getDataflowService()
						.projects()
						.locations()
						.jobs()
						.list(projectId, region)
						.execute();

				for (Job job : response.getJobs()) {
					LOG.debug(String.format("Comparing Dataflow job names %s and %s", jobName, job.getName()));
					if (job.getName().equals(jobName)) {
						return job;
					}
				}

				token = response.getNextPageToken();
			} while (token != null);

			Thread.sleep(10000);
		}

		throw new IOException(String.format("Job %s was not found for project %s", jobName, projectId));
	}

	public static void waitForJobToStart(DataflowPipelineOptions options) throws InterruptedException, GeneralSecurityException, IOException {
		String jobName = options.getJobName();
		long retryInterval = TimeUnit.SECONDS.toMillis(10);
		long maxAttempts1 = TimeUnit.MINUTES.toMillis(10) / retryInterval;
		long maxAttempts2 = TimeUnit.MINUTES.toMillis(10) / retryInterval;

		Job job = null;
		for (int attempts = 0; attempts < maxAttempts1; attempts++) {
			LOG.info(String.format("Waiting for Google Dataflow job %s to start...", jobName));
			job = getJob(options);
			if (job.getCurrentState().equals("JOB_STATE_RUNNING")) {
				break;
			} else {
				Thread.sleep(retryInterval);
			}
		}

		assertNotNull(job);
		LOG.info(String.format("Google Dataflow job %s has started", jobName));

		for (int attempts = 0; attempts < maxAttempts2; attempts++) {
			LOG.info(String.format("Waiting for Google Dataflow workers for job %s to start...", jobName));
			String workersStartedMessage = "Workers have started";
			if (findJobMessage(job.getProjectId(), job.getId(), options.getRegion(), workersStartedMessage) != null) {
				break;
			} else {
				Thread.sleep(retryInterval);
			}
		}

		LOG.info(String.format("Google Dataflow workers for job %s has started", jobName));
	}

	public static JobMessage findJobMessage(String projectId, String jobId, String region, String substring)
			throws IOException, GeneralSecurityException {
		Dataflow.Projects.Locations.Jobs.Messages.List request = getDataflowService()
				.projects()
				.locations()
				.jobs()
				.messages()
				.list(projectId, region, jobId);

		ListJobMessagesResponse response;
		JobMessage matchedMessage;
		do {
			response = request.execute();
			matchedMessage = response.getJobMessages()
					.stream()
					.filter(m -> m.getMessageText().toLowerCase().contains(substring.toLowerCase()))
					.findAny()
					.orElse(null);
		} while (matchedMessage == null && response.getNextPageToken() != null);
		return matchedMessage;
	}

	public static JobMetrics getJobMetrics(String projectId, String jobId) throws IOException, GeneralSecurityException {
		return getDataflowService()
				.projects()
				.jobs()
				.getMetrics(projectId, jobId)
				.execute();
	}

	private static Dataflow getDataflowService() throws IOException, GeneralSecurityException {
		GoogleCredential credentials = GoogleCredential.getApplicationDefault();

		if (credentials.createScopedRequired()) {
			credentials = credentials.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
		}

		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
		return new Dataflow.Builder(httpTransport, jsonFactory, credentials)
				.setApplicationName("Google Cloud Platform Sample")
				.build();
	}
}
