package org.apache.beam.sdk.io.solace;

import com.solacesystems.jcsmp.BytesXMLMessage;
import java.nio.charset.StandardCharsets;

public class StringMessageMapper implements SolaceIO.MessageMapper<String> {
  private static final long serialVersionUID = 42L;

  @Override
  public String mapMessage(BytesXMLMessage message) throws Exception {
    return new String(message.getBytes(), StandardCharsets.UTF_8);
  }
}
