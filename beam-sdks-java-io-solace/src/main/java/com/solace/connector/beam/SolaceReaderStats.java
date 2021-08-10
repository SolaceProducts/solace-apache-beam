package com.solace.connector.beam;

import com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


/**
 * Group of statistics to show progress for events into and out of the UnboundedSolaceReader.
 */
@VisibleForTesting
class SolaceReaderStats implements Serializable {
	private static final long serialVersionUID = 42L;
	private static final Logger LOG = LoggerFactory.getLogger(SolaceReaderStats.class);

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


	public SolaceReaderStats() {
		this.zeroStats();
	}

	private void zeroStats() {
		emptyPoll = 0L;
		msgRecieved = 0L;
		currentBacklogBytes = 0L;
		checkpointReadyMessages = 0L;
		checkpointCompleteMessages = 0L;
		pollFlowRebind = 0L;
		messagesRemovedFromCheckpointQueue = 0L;
		monitorChecks = 0L;
		monitorFlowClose = 0L;
	}

	public void incrementEmptyPoll() {
		emptyPoll++;
	}

	public void incrementMessageReceived() {
		msgRecieved++;
	}

	public void incrementPollFlowRebind() {
		pollFlowRebind++;
	}

	public void incrementMessagesRemovedFromCheckpointQueue(long count) {
		messagesRemovedFromCheckpointQueue += count;
	}

	public void incrementMonitorChecks() {
		monitorChecks++;
	}

	public void incrementMonitorFlowClose() {
		monitorFlowClose++;
	}

	public void setLastReportTime(Instant time) {
		this.lastReportTime = time;
	}

	public Instant getLastReportTime() {
		return this.lastReportTime;
	}

	public void setCurrentAdvanceTime(Instant time) {
		this.currentAdvanceTime = time;
	}

	public void setCurrentCheckpointTime(Instant time) {
		this.currentCheckpointTime = time;
	}

	public void setCurrentBacklog(Long bytes) {
		this.currentBacklogBytes = bytes;
	}

	public void incrCheckpointReadyMessages(Long count) {
		checkpointReadyMessages = checkpointReadyMessages + count;
	}

	public void incrCheckpointCompleteMessages(Long count) {
		checkpointCompleteMessages = checkpointCompleteMessages + count;
	}

	public Long getEmptyPoll() {
		return emptyPoll;
	}

	public Long getMsgRecieved() {
		return msgRecieved;
	}

	public Long getCurrentBacklogBytes() {
		return currentBacklogBytes;
	}

	public Long getCheckpointReadyMessages() {
		return checkpointReadyMessages;
	}

	public Long getCheckpointCompleteMessages() {
		return checkpointCompleteMessages;
	}

	public Long getPollFlowRebind() {
		return pollFlowRebind;
	}

	public Long getMessagesRemovedFromCheckpointQueue() {
		return messagesRemovedFromCheckpointQueue;
	}

	public Long getMonitorChecks() {
		return monitorChecks;
	}

	public Long getMonitorFlowClose() {
		return monitorFlowClose;
	}

	public String dumpStatsAndClear(Boolean verbose) {
		String results = this.dumpStats(verbose);
		this.zeroStats();
		return results;
	}

	public String dumpStats(Boolean verbose) {
		String results = "{";
		results += "\"queueBacklog\":" + currentBacklogBytes + ",";
		results += "\"emptyPolls\":" + emptyPoll + ",";
		results += "\"messagesReceived\":" + msgRecieved + ",";
		results += "\"messagesCheckpointReady\":" + checkpointReadyMessages + ",";
		results += "\"messagesCheckpointComplete\":" + checkpointCompleteMessages + ",";
		results += "\"pollFlowRebind\":" + pollFlowRebind + ",";
		results += "\"messagesRemovedFromCheckpointQueue\":" + messagesRemovedFromCheckpointQueue + ",";
		results += "\"monitorChecks\":" + monitorChecks + ",";
		results += "\"monitorFlowClose\":" + monitorFlowClose + "}";
		return results;
	}

}