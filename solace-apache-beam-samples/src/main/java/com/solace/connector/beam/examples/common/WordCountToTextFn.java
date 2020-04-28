package com.solace.connector.beam.examples.common;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/**
 * This is the {@code FormatAsTextFn} Function from
 * <a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java">
 *     Apache Beam's WordCount example</a>.
 *
 * A SimpleFunction that converts a Word and Count into a printable string.
 */
public class WordCountToTextFn extends SimpleFunction<KV<String, Long>, String> {
	@Override
	public String apply(KV<String, Long> input) {
		return input.getKey() + ": " + input.getValue();
	}
}
