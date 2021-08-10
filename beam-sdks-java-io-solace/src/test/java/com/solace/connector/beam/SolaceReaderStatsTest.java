package com.solace.connector.beam;

import org.joda.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SolaceReaderStatsTest {
	private SolaceReaderStats solaceReaderStats;
	private Long emptyPoll;
	private Long msgRecieved;
	private Instant currentAdvanceTime;
	private Instant currentCheckpointTime;
	private Instant lastReportTime;
	private Long currentBacklogBytes;
	private Long checkpointReadyMessages;
	private Long checkpointCompleteMessages;
	private Long pollFlowRebind;
	private Long messagesRemovedFromCheckpointQueue;
	private Long monitorChecks;
	private Long monitorFlowClose;

	@BeforeEach
	public void setup() {
		Random random = new Random();

		emptyPoll = Math.abs(random.nextLong()) % 100;
		msgRecieved = Math.abs(random.nextLong())% 100;
		currentAdvanceTime = new Instant(Math.abs(random.nextLong()));
		currentCheckpointTime = new Instant(Math.abs(random.nextLong()));
		lastReportTime = new Instant(Math.abs(random.nextLong()));
		currentBacklogBytes = Math.abs(random.nextLong());
		checkpointReadyMessages = Math.abs(random.nextLong());
		checkpointCompleteMessages = Math.abs(random.nextLong());
		pollFlowRebind = Math.abs(random.nextLong()) % 100;
		messagesRemovedFromCheckpointQueue = Math.abs(random.nextLong());
		monitorChecks = Math.abs(random.nextLong())% 100;
		monitorFlowClose = Math.abs(random.nextLong())% 100;

		solaceReaderStats = new SolaceReaderStats();
		for (int i = 0; i < emptyPoll; i++) solaceReaderStats.incrementEmptyPoll();
		for (int i = 0; i < msgRecieved; i++) solaceReaderStats.incrementMessageReceived();
		solaceReaderStats.setCurrentAdvanceTime(currentAdvanceTime);
		solaceReaderStats.setCurrentCheckpointTime(currentCheckpointTime);
		solaceReaderStats.setLastReportTime(lastReportTime);
		solaceReaderStats.setCurrentBacklog(currentBacklogBytes);
		solaceReaderStats.incrCheckpointReadyMessages(checkpointReadyMessages);
		solaceReaderStats.incrCheckpointCompleteMessages(checkpointCompleteMessages);
		for (int i = 0; i < pollFlowRebind; i++) solaceReaderStats.incrementPollFlowRebind();
		solaceReaderStats.incrementMessagesRemovedFromCheckpointQueue(messagesRemovedFromCheckpointQueue);
		for (int i = 0; i < monitorChecks; i++) solaceReaderStats.incrementMonitorChecks();
		for (int i = 0; i < monitorFlowClose; i++) solaceReaderStats.incrementMonitorFlowClose();
	}

	@Test
	public void testInit() {
		solaceReaderStats = new SolaceReaderStats();
		validateEmpty(solaceReaderStats.dumpStats(false));
		assertNull(solaceReaderStats.getLastReportTime());
	}

	@Test
	public void testDump() {
		validate(solaceReaderStats.dumpStats(false));
	}

	@Test
	public void testVerboseDump() {
		assertEquals(solaceReaderStats.dumpStats(false), solaceReaderStats.dumpStats(true));
	}

	@Test
	public void testDumpAndClear() {
		validate(solaceReaderStats.dumpStatsAndClear(false));
		validateEmpty(solaceReaderStats.dumpStats(false));
	}

	public void validate(String dump) {
		assertThat(dump, containsString(String.format("\"queueBacklog\":%s", currentBacklogBytes)));
		assertThat(dump, containsString(String.format("\"emptyPolls\":%s", emptyPoll)));
		assertThat(dump, containsString(String.format("\"messagesReceived\":%s", msgRecieved)));
		assertThat(dump, containsString(String.format("\"messagesCheckpointReady\":%s", checkpointReadyMessages)));
		assertThat(dump, containsString(String.format("\"messagesCheckpointComplete\":%s", checkpointCompleteMessages)));
		assertThat(dump, containsString(String.format("\"pollFlowRebind\":%s", pollFlowRebind)));
		assertThat(dump, containsString(String.format("\"messagesRemovedFromCheckpointQueue\":%s", messagesRemovedFromCheckpointQueue)));
		assertThat(dump, containsString(String.format("\"monitorChecks\":%s", monitorChecks)));
		assertThat(dump, containsString(String.format("\"monitorFlowClose\":%s", monitorFlowClose)));
		assertEquals(lastReportTime, solaceReaderStats.getLastReportTime());
	}

	public void validateEmpty(String dump) {
		String expected = "{";
		expected += "\"queueBacklog\":0,";
		expected += "\"emptyPolls\":0,";
		expected += "\"messagesReceived\":0,";
		expected += "\"messagesCheckpointReady\":0,";
		expected += "\"messagesCheckpointComplete\":0,";
		expected += "\"pollFlowRebind\":0,";
		expected += "\"messagesRemovedFromCheckpointQueue\":0,";
		expected += "\"monitorChecks\":0,";
		expected += "\"monitorFlowClose\":0}";

		assertEquals(expected, dump);
	}
}
