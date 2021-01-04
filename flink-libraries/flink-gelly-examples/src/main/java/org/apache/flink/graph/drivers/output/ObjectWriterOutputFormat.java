/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.drivers.output;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;

import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;

/**
 * A {@link FileOutputFormat} that writes objects to a text file as encoded by
 * the given Jackson {@link ObjectWriter}.
 *
 * @param <IT> The type of the consumed records.
 */
public class ObjectWriterOutputFormat<IT> extends FileOutputFormat<IT> {

	private final ObjectWriter writer;

	/**
	 * Initialize with a Jackson {@link ObjectWriter} configured with the
	 * output format (JSON, XML, CSV, etc.) and features (separators, etc.).
	 *
	 * @param outputPath the file output path
	 * @param writer the object writer
	 */
	public ObjectWriterOutputFormat(Path outputPath, ObjectWriter writer) {
		super(outputPath);

		this.writer = writer;
	}

	@Override
	public void writeRecord(IT record) throws IOException {
		writer.writeValue(stream, record);
	}
}
