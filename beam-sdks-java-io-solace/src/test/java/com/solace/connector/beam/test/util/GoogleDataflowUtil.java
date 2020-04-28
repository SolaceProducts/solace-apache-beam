package com.solace.connector.beam.test.util;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class GoogleDataflowUtil extends GoogleUtilBase {
	private static final Logger LOG = LoggerFactory.getLogger(GoogleDataflowUtil.class);
	private static Dataflow dataflowService;
	private static final String JOB_STATE_DRAINED = "JOB_STATE_DRAINED";
	private static final String JOB_STATE_DRAINING = "JOB_STATE_DRAINING";
	private static final String JOB_STATE_RUNNING = "JOB_STATE_RUNNING";
	private static final String JOB_STATE_UPDATED = "JOB_STATE_UPDATED";

	private GoogleDataflowUtil() {}

	public static Job getJob(String projectId, String region, String jobId)
			throws IOException, GeneralSecurityException {
		return getService()
				.projects()
				.locations()
				.jobs()
				.get(projectId, region, jobId)
				.execute();
	}

	public static Job getJob(DataflowPipelineOptions options)
			throws IOException, GeneralSecurityException, InterruptedException {

		String jobName = options.getJobName();
		String projectId = options.getProject();
		String region = options.getRegion();

		for (int attempt = 0; attempt < 60; attempt++) {
			LOG.info(String.format("Getting Google Dataflow info for job name %s of project ID %s in region %s",
					jobName, projectId, region));
			String pageToken = null;
			do {
				ListJobsResponse response = getService()
						.projects()
						.locations()
						.jobs()
						.list(projectId, region)
						.setPageToken(pageToken)
						.execute();

				for (Job job : response.getJobs()) {
					LOG.debug(String.format("Comparing Dataflow job names %s and %s", jobName, job.getName()));
					if (job.getName().equals(jobName)) {
						return job;
					}
				}

				pageToken = response.getNextPageToken();
			} while (pageToken != null);

			Thread.sleep(10000);
		}

		throw new IOException(String.format("Job %s was not found for project %s", jobName, projectId));
	}

	public static void drainJob(DataflowPipelineOptions options) throws InterruptedException, GeneralSecurityException, IOException {
		Job job = getJob(options);
		getService()
				.projects()
				.jobs()
				.update(job.getProjectId(), job.getId(), new Job().setRequestedState(JOB_STATE_DRAINED))
				.execute();

		String state = JOB_STATE_DRAINING;
		for (long wait = TimeUnit.MINUTES.toMillis(10), sleep = TimeUnit.SECONDS.toMillis(10);
			 (state.equals(JOB_STATE_DRAINING) || state.equals(JOB_STATE_RUNNING)) && wait > 0; wait -= sleep) {
			LOG.info(String.format("Waiting for job %s to be drained - %s sec remaining",
					job.getName(), TimeUnit.MILLISECONDS.toSeconds(wait)));
			sleep = Math.min(sleep, wait);
			Thread.sleep(sleep);
			state = getJob(options).getCurrentState();
		}

		if (!state.equals(JOB_STATE_DRAINED)) {
			throw new IllegalStateException(String.format("job %s didn't reach state %s, current state is %s",
					job.getName(), JOB_STATE_DRAINED, state));
		}
	}

	public static void waitForJobToStart(String projectId, String region, String jobId)
			throws InterruptedException, GeneralSecurityException, IOException {
		waitForJobToStart(() -> getJob(projectId, region, jobId));
	}

	public static void waitForJobToStart(DataflowPipelineOptions options)
			throws InterruptedException, GeneralSecurityException, IOException {
		waitForJobToStart(() -> getJob(options));
	}

	private static void waitForJobToStart(ThrowingSupplier<Job> jobSupplier)
			throws InterruptedException, GeneralSecurityException, IOException {
		long retryInterval = TimeUnit.SECONDS.toMillis(10);
		long maxAttempts1 = TimeUnit.MINUTES.toMillis(10) / retryInterval;
		long maxAttempts2 = TimeUnit.MINUTES.toMillis(10) / retryInterval;

		Job job = null;
		for (int attempts = 0; attempts < maxAttempts1; attempts++) {
			job = jobSupplier.get();
			LOG.info(String.format("Waiting for Google Dataflow job %s to start...", job.getName()));
			if (job.getCurrentState().equals(JOB_STATE_RUNNING)) {
				break;
			} else {
				Thread.sleep(retryInterval);
			}
		}

		assertNotNull(job);
		assertEquals(JOB_STATE_RUNNING, job.getCurrentState());
		LOG.info(String.format("Google Dataflow job %s has started", job.getName()));

		for (int attempts = 0; attempts < maxAttempts2; attempts++) {
			LOG.info(String.format("Waiting for Google Dataflow workers for job %s to start...", job.getName()));
			String workersStartedMessage = "Workers have started";
			if (findJobMessage(job.getProjectId(), job.getId(), job.getLocation(), workersStartedMessage) != null) {
				break;
			} else {
				Thread.sleep(retryInterval);
			}
		}

		LOG.info(String.format("Google Dataflow workers for job %s has started", job.getName()));
	}

	public static void waitForJobToUpdate(Job updatingJob)
			throws InterruptedException, GeneralSecurityException, IOException {
		waitForJobToUpdate(updatingJob.getProjectId(), updatingJob.getLocation(), updatingJob.getId());
	}

	public static void waitForJobToUpdate(String projectId, String region, String updatingJobId)
			throws IOException, GeneralSecurityException, InterruptedException {
		long retryInterval = TimeUnit.SECONDS.toMillis(10);
		long maxAttempts1 = TimeUnit.MINUTES.toMillis(10) / retryInterval;

		Job job = null;
		for (int attempts = 0; attempts < maxAttempts1; attempts++) {
			LOG.info(String.format("Waiting for Google Dataflow job id %s to reach state %s",
					updatingJobId, JOB_STATE_UPDATED));
			job = getJob(projectId, region, updatingJobId);
			if (job.getCurrentState().equals(JOB_STATE_UPDATED)) {
				break;
			} else {
				Thread.sleep(retryInterval);
			}
		}

		assertNotNull(job);
		assertEquals(JOB_STATE_UPDATED, job.getCurrentState());
		LOG.info(String.format("Google Dataflow job ID %s has reached state %s", updatingJobId, JOB_STATE_UPDATED));

		String newJobId = job.getReplacedByJobId();
		LOG.info(String.format("Google Dataflow job ID %s was updated into ID %s", updatingJobId, newJobId));
		waitForJobToStart(projectId, region, newJobId);
	}

	public static JobMessage findJobMessage(String projectId, String jobId, String region, String substring)
			throws IOException, GeneralSecurityException {
		String pageToken = null;
		JobMessage matchedMessage;
		do {
			ListJobMessagesResponse response = getService()
					.projects()
					.locations()
					.jobs()
					.messages()
					.list(projectId, region, jobId)
					.setPageToken(pageToken)
					.execute();
			matchedMessage = response.getJobMessages()
					.stream()
					.filter(m -> m.getMessageText().toLowerCase().contains(substring.toLowerCase()))
					.findAny()
					.orElse(null);
			pageToken = response.getNextPageToken();
		} while (matchedMessage == null && pageToken != null);
		return matchedMessage;
	}

	public static JobMetrics getJobMetrics(String projectId, String jobId) throws IOException, GeneralSecurityException {
		return getService()
				.projects()
				.jobs()
				.getMetrics(projectId, jobId)
				.execute();
	}

	private static Dataflow getService() throws IOException, GeneralSecurityException {
		if (dataflowService == null) {
			GoogleCredential credentials = GoogleCredential.getApplicationDefault();

			if (credentials.createScopedRequired()) {
				credentials = credentials.createScoped(Collections.singletonList(GOOGLE_AUTH_CLOUD_URL));
			}

			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
			dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, credentials)
					.setApplicationName("Google Cloud Platform Sample")
					.build();
		}
		return dataflowService;
	}
}
