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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.apache.avro.generic.GenericData.Record;
import static org.apache.parquet.cli.util.Expressions.filterSchema;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

public class ParquetWriter {

    List<String> columns;
    boolean overwrite = true;
    boolean v2 = true;
    int rowGroupSize = DEFAULT_BLOCK_SIZE;
    int pageSize = DEFAULT_PAGE_SIZE;
    int dictionaryPageSize = DEFAULT_PAGE_SIZE;

    public void write(List<Map<String, Object>> items, String outputPath, Schema schema, CompressionCodecName codec) throws IOException {
        Schema projection = filterSchema(schema, columns);

        Path outPath = new Path(outputPath);
        Configuration conf = new Configuration();
        FileSystem outFS = outPath.getFileSystem(conf);
        if (overwrite && outFS.exists(outPath)) {
            outFS.delete(outPath, true);
        }

        try (org.apache.parquet.hadoop.ParquetWriter<Record> writer = AvroParquetWriter
                .<Record>builder(new Path(outputPath))
                .withWriterVersion(v2 ? PARQUET_2_0 : PARQUET_1_0)
                .withConf(conf)
                .withCompressionCodec(codec)
                .withRowGroupSize(rowGroupSize)
                .withDictionaryPageSize(dictionaryPageSize < 64 ? 64 : dictionaryPageSize)
                .withDictionaryEncoding(dictionaryPageSize != 0)
                .withPageSize(pageSize)
                .withDataModel(GenericData.get())
                .withSchema(projection)
                .build()) {
            items.forEach(item -> {
                try {
                    Record record = new Record(schema);
                    schema.getFields().forEach(field -> {
                        int index = field.pos();
                        Object value = item.get(field.name());
                        if (value instanceof String) {
                            value = ByteBuffer.wrap(((String) value).getBytes(ISO_8859_1));
                        }
                        record.put(index, value);
                    });
                    writer.write(record);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
