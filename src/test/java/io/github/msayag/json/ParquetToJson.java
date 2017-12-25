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

import io.github.msayag.parquet.ParquetReader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ParquetToJson {
    @Test
    void testConvert() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File jsonInputFile = new File(classLoader.getResource("samples/oecdpop.parquet").getFile());
        String cvsOutputFile = File.createTempFile("popFromJson", ".json").getAbsolutePath();
        List<Map<String, Object>> records = new ParquetReader().read(jsonInputFile.getAbsolutePath());

        System.out.println("cvsOutputFile: " + cvsOutputFile);
        new JsonWriter().write(records, cvsOutputFile);
    }
}
