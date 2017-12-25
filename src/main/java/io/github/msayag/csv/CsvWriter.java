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
package io.github.msayag.csv;

import io.github.msayag.Writer;
import org.apache.avro.Schema;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class CsvWriter implements Writer {
    private Schema schema;
    private boolean createHeader;

    public CsvWriter(Schema schema, boolean createHeader) {
        this.schema = schema;
        this.createHeader = createHeader;
    }

    @Override
    public void write(List<Map<String, Object>> records, String csvOutputFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(csvOutputFile);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos))) {
            List<String> fieldNames = getFieldNames(schema);
            if (createHeader) {
                createHeader(writer, fieldNames);
            }
            records.stream()
                    .map(record -> toString(record, fieldNames))
                    .forEach(line -> {
                        try {
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private List<String> getFieldNames(Schema schema) {
        return schema.getFields().stream()
                .map(field -> field.name())
                .collect(toList());
    }

    private void createHeader(BufferedWriter writer, List<String> fieldNames) throws IOException {
        String header = fieldNames.stream().collect(joining(", "));
        writer.write(header);
        writer.newLine();
    }

    private String toString(Map<String, Object> record, List<String> fieldNames) {
        return fieldNames.stream()
                .map(name -> record.get(name))
                .map(field -> String.valueOf(field))
                .collect(joining(", "));
    }
}
