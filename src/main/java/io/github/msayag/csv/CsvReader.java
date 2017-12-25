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
 * Based on https://github.com/Parquet/parquet-compatibility/blob/master/parquet-compat/src/test/java/parquet/compat/test/ConvertUtils.java
 */

package io.github.msayag.csv;

import com.opencsv.CSVReader;
import io.github.msayag.Reader;
import org.apache.avro.Schema;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class CsvReader implements Reader {
    private Schema schema;
    private boolean hasHeader;

    public CsvReader(Schema schema, boolean hasHeader) {
        this.schema = schema;
        this.hasHeader = hasHeader;
    }

    @Override
    public List<Map<String, Object>> read(String csvFile) throws IOException {
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            if (hasHeader) {
                skipHeader(reader);
            }
            String[] fieldNames = extractFieldNames(schema);
            Schema[] fieldTypes = extractFieldTypes(schema);
            return StreamSupport.stream(reader.spliterator(), false)
                    .map(fields -> toMap(fields, fieldNames, fieldTypes))
                    .collect(toList());
        }
    }

    private String[] extractFieldNames(Schema schema) {
        return schema.getFields().stream()
                .map(Schema.Field::name)
                .toArray(String[]::new);
    }

    private Schema[] extractFieldTypes(Schema schema) {
        return schema.getFields().stream()
                .map(field -> field.schema())
                .toArray(Schema[]::new);
    }

    private void skipHeader(CSVReader reader) throws IOException {
        reader.readNext();
    }

    private Map<String, Object> toMap(String[] fields, String[] fieldNames, Schema[] fieldTypes) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            String key = fieldNames[i].trim();
            Schema type = fieldTypes[i];
            Object value = cast(fields[i].trim(), type);
            result.put(key, value);
        }
        return result;
    }

    private Object cast(String value, Schema type) {
        switch (type.getType()) {
            case BYTES:
                return value;
            case INT:
                return Integer.valueOf(value);
            case DOUBLE:
                return Double.valueOf(value);
            case ENUM:
                return value;
        }
        throw new RuntimeException("Unsupported schema type: " + type);
    }
}
