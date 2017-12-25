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

import io.github.msayag.ParquetReader;
import io.github.msayag.ParquetWriter;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonToParquetConverter {
    public void convert(String jsonFile, String outputParquetFile, Schema schema, CompressionCodecName codec) throws IOException {
        List<Map<String, Object>> records = new ParquetReader().read(jsonFile);
        new ParquetWriter().write(records, outputParquetFile, schema, codec);
    }
}
