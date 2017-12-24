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
package io.github.msayag;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.cli.util.Expressions;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.cli.util.Expressions.select;

public class ParquetReader {

    public List<Map<String, Object>> read(String sourceFile) throws IOException {
        return read(sourceFile, Integer.MAX_VALUE);
    }

    public List<Map<String, Object>> read(String source, long numRecords) throws IOException {
        return read(source, numRecords, List.of());
    }

    public List<Map<String, Object>> read(String source, long numRecords, List<String> columns) throws IOException {
        ParquetUtil util = new ParquetUtil();
        Schema schema = util.getAvroSchema(source);
        Schema projection = Expressions.filterSchema(schema, columns);

        Iterable<Object> reader = util.openDataFile(source, projection);
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

