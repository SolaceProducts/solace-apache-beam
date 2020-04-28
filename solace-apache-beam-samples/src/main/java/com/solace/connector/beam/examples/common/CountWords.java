package com.solace.connector.beam.examples.common;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * This is the {@link CountWords} PTransform from
 * <a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java">
 *     Apache Beam's WordCount example</a>.
 *
 * <p>A PTransform that converts a PCollection containing lines of text into a PCollection of
 * formatted word counts.
 *
 * <p>This is a custom composite transform that bundles two transforms (ParDo and
 * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
 * modular testing, and an improved monitoring experience.
 */
public class CountWords
		extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
	/**
	 * You can make your pipeline assembly code less verbose by defining your DoFns
	 * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
	 * a ParDo in the pipeline.
	 */
	static class ExtractWordsFn extends DoFn<String, String> {
		private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
		private final Distribution lineLenDist =
				Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<String> receiver) {
			lineLenDist.update(element.length());
			if (element.trim().isEmpty()) {
				emptyLines.inc();
			}

			// Split the line into words.
			String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					receiver.output(word);
				}
			}
		}
	}

	@Override
	public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

		// Convert lines of text into individual words.
		PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

		// Count the number of times each word occurs.
		PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

		return wordCounts;
	}
}
