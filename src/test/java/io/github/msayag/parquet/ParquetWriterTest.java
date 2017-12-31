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

import io.github.msayag.AvroSchemaExtractor;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetWriterTest {
    @ParameterizedTest
    @MethodSource("schemaAndColumnsProvider")
    void testExtractColumns(String schemaFileName, List<String> columns) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File parquetInputFile = new File(classLoader.getResource("samples/oecdpop.parquet").getFile());
        String tmpFilePrefix = File.createTempFile("popFromParquet", "").getAbsolutePath();
        List<Map<String, Object>> records = new ParquetReader().read(parquetInputFile.getAbsolutePath());
        String parquetOutputFile = tmpFilePrefix + ".parquet";
        System.out.println("parquetOutputFile: " + parquetOutputFile);
        String schemaFile = classLoader.getResource(schemaFileName).getFile();
        Schema schema = new AvroSchemaExtractor().getAvroSchema(schemaFile);
        new ParquetWriter(schema, CompressionCodecName.GZIP).write(records, parquetOutputFile, columns);

        List<Map<String, Object>> records2 = new ParquetReader().read(parquetOutputFile);
        assertEquals(records.size(), records2.size());
        assertEquals(columns.size(), records2.get(0).size());
    }

    static Stream<Arguments> schemaAndColumnsProvider() {
        return Stream.of(
                Arguments.of("samples/oecdpop.1.avsc", List.of("location")),
                Arguments.of("samples/oecdpop.2.avsc", List.of("location", "value"))
        );
    }
}