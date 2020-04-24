package com.solace.connector.beam.test.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountMessagesPTransform extends PTransform<PCollection<String>, PCollection<Long>> {
	private static final Logger LOG = LoggerFactory.getLogger(CountMessagesPTransform.class);

	@Override
	public PCollection<Long> expand(PCollection<String> lines) {
		// Count the number of times each word occurs.
		return lines.apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())
				.apply(ParDo.of(new DoFn<Long, Long>() {
					@ProcessElement
					public void processElement(@Element Long count, OutputReceiver<Long> receiver) {
						LOG.info(String.format("Messages Received Count: %s", count));
						receiver.output(count);
					}
				}));
	}
}
