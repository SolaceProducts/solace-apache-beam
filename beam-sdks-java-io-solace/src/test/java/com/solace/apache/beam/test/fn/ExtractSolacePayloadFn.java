package com.solace.apache.beam.test.fn;

import com.solace.apache.beam.SolaceTestRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractSolacePayloadFn extends DoFn<SolaceTestRecord, String> {
	private static final Logger LOG = LoggerFactory.getLogger(ExtractSolacePayloadFn.class);

	@ProcessElement
	public void processElement(@Element SolaceTestRecord record, OutputReceiver<String> receiver) {
		LOG.info(String.format("Received message from queue %s:\n%s", record.getDestination(), record));
		receiver.output(record.getPayload());
	}
}
