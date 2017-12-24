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

import io.github.msayag.ParquetUtil;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

class CsvRoundTripTest {
    @Test
    void testReadWrite() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File csvInputFile = new File(classLoader.getResource("samples/oecdpop.csv").getFile());
        String tmpFilePrefix = File.createTempFile("popFromCsv", "").getAbsolutePath();
        String parquetTmpFile = tmpFilePrefix + ".parquet";
        String schemaFile = classLoader.getResource("samples/oecdpop.avsc").getFile();
        Schema schema = new ParquetUtil().getAvroSchema(schemaFile);
        new CsvToParquetConverter().convert(csvInputFile.getAbsolutePath(), true, parquetTmpFile, schema, CompressionCodecName.GZIP);
        String csvOutputFile = tmpFilePrefix + ".csv";
        System.out.println("parquetTmpFile: " + parquetTmpFile);
        System.out.println("csvOutputFile: " + csvOutputFile);
        new ParquetToCsvConverter().convert(parquetTmpFile, csvOutputFile, true);
    }
}