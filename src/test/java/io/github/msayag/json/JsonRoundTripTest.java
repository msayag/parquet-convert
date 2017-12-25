package io.github.msayag.json;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonRoundTripTest {
    @Test
    void testReadWrite() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File jsonInputFile = new File(classLoader.getResource("samples/oecdpop.json").getFile());
        String tmpFilePrefix = File.createTempFile("popFromJson", "").getAbsolutePath();
        List<Map<String, Object>> records = new JsonReader().read(jsonInputFile.getAbsolutePath());
        String jsonOutputFile = tmpFilePrefix + ".json";
        System.out.println("jsonOutputFile: " + jsonOutputFile);
        new JsonWriter().write(records, jsonOutputFile);
    }
}
