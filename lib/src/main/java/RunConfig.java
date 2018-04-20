import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class RunConfig {
    private Map<String, Object> values;

    public RunConfig(Map<String, Object> values) {
        this.values = values;
    }

    public static RunConfig fromJsonFile(String file) throws IOException {
        BufferedReader r = new BufferedReader(new FileReader(file));
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(
                r,
                new TypeReference<Map<String, Object>>() {}
                );
        return new RunConfig(map);
    }

    public static RunConfig fromJsonString(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(
                json,
                new TypeReference<Map<String, Object>>() {}
        );
        return new RunConfig(map);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) values.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, T defaultValue) {
        return (T) values.getOrDefault(key, defaultValue);
    }

    public Map<String, Object> getValues() {
        return values;
    }
}
