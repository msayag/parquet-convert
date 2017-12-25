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

package io.github.msayag.json;

import io.github.msayag.AvroSchemaExtractor;
import io.github.msayag.parquet.ParquetWriter;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonToParquet {
    @Test
    void testConvert() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File jsonInputFile = new File(classLoader.getResource("samples/oecdpop.json").getFile());
        List<Map<String, Object>> records = new JsonReader().read(jsonInputFile.getAbsolutePath());

        String parquetOutputFile = File.createTempFile("popFromJson", ".parquet").getAbsolutePath();
        System.out.println("parquetOutputFile: " + parquetOutputFile);

        String schemaFile = classLoader.getResource("samples/oecdpop.avsc").getFile();
        Schema schema = new AvroSchemaExtractor().getAvroSchema(schemaFile);
        new ParquetWriter(schema, CompressionCodecName.GZIP).write(records, parquetOutputFile);
    }
}
