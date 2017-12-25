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

import com.google.common.io.CharStreams;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.cli.json.AvroJsonReader;
import org.apache.parquet.cli.util.Formats;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ParquetUtil {
    private Configuration conf = new Configuration();

    protected <D> Iterable<D> openDataFile(final String source, Schema projection)
            throws IOException {
        Formats.Format format;
        try (InputStream in = new FileInputStream(source)) {
            format = Formats.detectFormat(in);
        }
        switch (format) {
            case PARQUET:
                // TODO: add these to the reader builder
                AvroReadSupport.setRequestedProjection(conf, projection);
                AvroReadSupport.setAvroReadSchema(conf, projection);
                final ParquetReader<D> parquet = AvroParquetReader.<D>builder(new Path(source))
                        .disableCompatibility()
                        .withDataModel(GenericData.get())
                        .withConf(conf)
                        .build();
                return new Iterable<>() {
                    @Override
                    public Iterator<D> iterator() {
                        return new Iterator<>() {
                            private boolean hasNext = false;
                            private D next = advance();

                            @Override
                            public boolean hasNext() {
                                return hasNext;
                            }

                            @Override
                            public D next() {
                                if (!hasNext) {
                                    throw new NoSuchElementException();
                                }
                                D toReturn = next;
                                this.next = advance();
                                return toReturn;
                            }

                            private D advance() {
                                try {
                                    D next = parquet.read();
                                    this.hasNext = (next != null);
                                    return next;
                                } catch (IOException e) {
                                    throw new RuntimeException(
                                            "Failed while reading Parquet file: " + source, e);
                                }
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException("Remove is not supported");
                            }
                        };
                    }
                };

            case AVRO:
                return DataFileReader.openReader(new File(source), new GenericDatumReader<>(projection));

            default:
                InputStream in = new FileInputStream(source);
                if (source.endsWith("json")) {
                    return new AvroJsonReader<>(in, projection);
                } else {
                    try {
                        return (Iterable<D>) CharStreams.readLines(new InputStreamReader(in));
                    } finally {
                        in.close();
                    }
                }
        }
    }

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
