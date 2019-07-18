package org.apache.beam.sdk.io.solace;

import com.google.common.annotations.VisibleForTesting;

import org.apache.beam.sdk.io.UnboundedSource;

import java.io.IOException;
import java.io.Serializable;

import javax.annotation.Nullable;

import com.solacesystems.jcsmp.BytesXMLMessage;

/**
 * Checkpoint for an unbounded Solace source. Consists of the Solace messages waiting to be
 * acknowledged and oldest pending message timestamp.
 */
@VisibleForTesting
class SolaceCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
  private static final long serialVersionUID = 42L;

  @Nullable private transient UnboundedSolaceReader reader;
  private String clientName;
  private BytesXMLMessage message;


  public SolaceCheckpointMark(UnboundedSolaceReader reader, String clientName, BytesXMLMessage message) {
    this.reader = reader;
    this.clientName = clientName;
    this.message = message;
  }


  @Override
  public void finalizeCheckpoint() throws IOException {
    if (reader != null) {
      reader.ackMessages(message);
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
