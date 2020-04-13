/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.solace.apache.beam.test.transform;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class FileWriterPTransform extends PTransform<PCollection<String>, PDone> {
	private static final long serialVersionUID = 42L;
	private String filenamePrefix;

	public FileWriterPTransform(String filenamePrefix) {
		this.filenamePrefix = filenamePrefix;
	}

	@Override
	public PDone expand(PCollection<String> input) {
		ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
		TextIO.Write write = TextIO.write()
				.to(resourceId)
				.withTempDirectory(resourceId.getCurrentDirectory())
				.withWindowedWrites()
				.withNumShards(1);
		return input.apply(write);
	}
}
