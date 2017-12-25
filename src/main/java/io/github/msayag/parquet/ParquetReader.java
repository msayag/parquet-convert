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
package io.github.msayag.parquet;

import com.google.common.io.CharStreams;
import io.github.msayag.AvroSchemaExtractor;
import io.github.msayag.Reader;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.cli.json.AvroJsonReader;
import org.apache.parquet.cli.util.Expressions;
import org.apache.parquet.cli.util.Formats;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.cli.util.Expressions.select;

public class ParquetReader implements Reader {

    @Override
    public List<Map<String, Object>> read(String sourceFile) throws IOException {
        return read(sourceFile, Integer.MAX_VALUE);
    }

    public List<Map<String, Object>> read(String source, long numRecords) throws IOException {
        return read(source, numRecords, List.of());
    }

    public List<Map<String, Object>> read(String source, long numRecords, List<String> columns) throws IOException {
        Schema schema = new AvroSchemaExtractor().getAvroSchema(source);
        Schema projection = Expressions.filterSchema(schema, columns);

        Iterable<Object> reader = openDataFile(source, projection);
        try {
            return StreamSupport.stream(reader.spliterator(), false)
                    .limit(numRecords)
                    .map(record -> {
                        if (columns == null || columns.size() != 1) {
                            return toMap(record);
                        } else {
                            String column = columns.get(0);
                            Object value = select(projection, record, column);
                            return Map.of(column, value);
                        }
                    }).collect(toList());
        } finally {
            if (reader instanceof Closeable) {
                ((Closeable) reader).close();
            }
        }
    }

    private  <D> Iterable<D> openDataFile(final String source, Schema projection)
            throws IOException {
        Formats.Format format;
        try (InputStream in = new FileInputStream(source)) {
            format = Formats.detectFormat(in);
        }
        switch (format) {
            case PARQUET:
                Configuration conf = new Configuration();
                AvroReadSupport.setRequestedProjection(conf, projection);
                AvroReadSupport.setAvroReadSchema(conf, projection);
                final org.apache.parquet.hadoop.ParquetReader<D> parquet = AvroParquetReader.<D>builder(new Path(source))
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

    private Map<String, Object> toMap(Object o) {
        GenericData.Record record = (GenericData.Record) o;
        Schema schema = record.getSchema();
        List<Schema.Field> fields = schema.getFields();
        return fields.stream()
                .collect(Collectors.toMap(
                        field -> field.name(),
                        field -> toObject(record.get(field.pos()))
                ));
    }

    private Object toObject(Object value) {
        if (value instanceof ByteBuffer) {
            byte[] bytes = ((ByteBuffer) value).array();
            return new String(bytes, ISO_8859_1);
        }
        return value;
    }
}

