package io.github.msayag.json;

import io.github.msayag.ParquetUtil;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class JsonRoundTripTest {
    @Test
    void testReadWrite() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File jsonInputFile = new File(classLoader.getResource("samples/oecdpop.json").getFile());
        String tmpFilePrefix = File.createTempFile("popFromJson", "").getAbsolutePath();
        String parquetTmpFile = tmpFilePrefix + ".parquet";
        String schemaFile = classLoader.getResource("samples/oecdpop.avsc").getFile();
        Schema schema = new ParquetUtil().getAvroSchema(schemaFile);
        new JsonToParquetConverter().convert(jsonInputFile.getAbsolutePath(), parquetTmpFile, schema, CompressionCodecName.GZIP);
        String jsonOutputFile = tmpFilePrefix + ".json";
        System.out.println("parquetTmpFile: " + parquetTmpFile);
        System.out.println("jsonOutputFile: " + jsonOutputFile);
        new ParquetToJsonConverter().convert(parquetTmpFile, jsonOutputFile);
    }
}
