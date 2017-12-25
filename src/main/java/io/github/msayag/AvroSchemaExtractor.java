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

/*
 * Based on parquet-cli:org.apache.parquet.cli.BaseCommand
 */
package io.github.msayag;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.cli.util.Formats;
import org.apache.parquet.cli.util.Schemas;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class AvroSchemaExtractor {
    private Configuration conf = new Configuration();

    public Schema getAvroSchema(String source) throws IOException {
        try (InputStream in = new FileInputStream(source)) {
            Formats.Format format = getFormat(source);
            switch (format) {
                case PARQUET:
                    return Schemas.fromParquet(conf, new File(source).toURI());
                case AVRO:
                    return Schemas.fromAvro(in);
                case TEXT:
                    if (source.endsWith("avsc")) {
                        return Schemas.fromAvsc(in);
                    } else if (source.endsWith("json")) {
                        return Schemas.fromJSON("json", in);
                    }
                default:
                    throw new IllegalArgumentException(String.format(
                            "Could not determine file format of %s.", source));
            }
        }
    }

    private Formats.Format getFormat(String source) throws IOException {
        try (InputStream in = new FileInputStream(source)) {
            return Formats.detectFormat(in);
        }
    }
}
