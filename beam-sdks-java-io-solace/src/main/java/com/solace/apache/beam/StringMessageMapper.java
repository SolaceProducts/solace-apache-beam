package com.solace.apache.beam;

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.nio.charset.StandardCharsets;

public class StringMessageMapper implements SolaceIO.InboundMessageMapper<String> {
  private static final long serialVersionUID = 42L;

  @Override
  public String map(BytesXMLMessage message) throws Exception {
    return new String(message.getBytes(), StandardCharsets.UTF_8);
  }
}
