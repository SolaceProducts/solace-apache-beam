package org.apache.beam.sdk.io.solace;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import org.joda.time.Instant;



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


    public SolaceReaderStats(){
        this.zeroStats();
    }

    private void zeroStats() {
        emptyPoll=0L;
        msgRecieved=0L;
        currentBacklogBytes=0L;
        checkpointReadyMessages=0L;
        checkpointCompleteMessages=0L;
    }

    public void incrementEmptyPoll() {
        emptyPoll++;
    }

    public void incrementMessageReceived() {
        msgRecieved++;
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

    public void incrCheckpointReadyMessages(Long count){
        checkpointReadyMessages = checkpointReadyMessages + count;
    }

    public void incrCheckpointCompleteMessages(Long count){
        checkpointCompleteMessages = checkpointCompleteMessages + count;
    }

    public String dumpStatsAndClear(Boolean verbose){
        String results=this.dumpStats(verbose);
        this.zeroStats();
        return results;
    }
    
    public String dumpStats(Boolean verbose) {
        String results="{";
        results += "\"queueBacklog\":"               + Long.toString(currentBacklogBytes)        + ",";
        results += "\"emptyPolls\":"                 + Long.toString(emptyPoll)                  + ",";
        results += "\"messagesReceived\":"           + Long.toString(msgRecieved)                + ",";
        results += "\"messagesCheckpointReady\":"    + Long.toString(checkpointReadyMessages)    + ",";
        results += "\"messagesCheckpointComplete\":" + Long.toString(checkpointCompleteMessages) + "}";
        return results;
    }

}