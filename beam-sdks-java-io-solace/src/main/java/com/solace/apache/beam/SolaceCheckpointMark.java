package com.solace.apache.beam;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.io.UnboundedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Checkpoint for an unbounded Solace source. Consists of the Solace messages waiting to be
 * acknowledged and oldest pending message timestamp.
 */
@VisibleForTesting
class SolaceCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
	private static final long serialVersionUID = 42L;
	private static final Logger LOG = LoggerFactory.getLogger(SolaceCheckpointMark.class);

	private final UUID id;
	@Nullable
	private transient UnboundedSolaceReader<?> reader;
	private String clientName;
	private transient BlockingQueue<UnboundedSolaceReader.Message> ackQueue;

	public SolaceCheckpointMark(UnboundedSolaceReader<?> reader,
								String clientName,
								BlockingQueue<UnboundedSolaceReader.Message> ackQueue) {
		this.id = UUID.randomUUID();
		this.reader = reader;
		this.clientName = clientName;
		this.ackQueue = ackQueue;
		LOG.debug(String.format("Created %s %s", this.getClass().getSimpleName(), this.id));
	}


	@Override
	public void finalizeCheckpoint() throws IOException {
		if (reader != null) {
			if (!reader.active.get()) {
				return;
			}
			LOG.debug(String.format("Started to finalize %s %s", this.getClass().getSimpleName(), this.id));

			int ackListSize = ackQueue.size();
			try {
				while (ackQueue.size() > 0) {
					UnboundedSolaceReader.Message msg = ackQueue.poll(0, TimeUnit.NANOSECONDS);

					if (msg != null) {
						msg.message.ackMessage();

						// advance watermark
						reader.watermark.updateAndGet(min -> Math.max(msg.time.getMillis(), min));
					}
				}
				reader.readerStats.incrCheckpointCompleteMessages((long) ackListSize);
			} catch (Exception e) {
				LOG.error(String.format("Got exception while acknowledging %s %s: %s",
						this.getClass().getSimpleName(), this.id, e.toString()), e);
				throw new IOException(e);
			}
		}
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof SolaceCheckpointMark) {
			SolaceCheckpointMark that = (SolaceCheckpointMark) other;
			return this.clientName.equals(that.clientName)
					&& (this.reader == that.reader);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		// Effective Java Item 11
		return clientName.hashCode() * 31 + System.identityHashCode(reader);
	}

}
