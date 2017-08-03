package edu.stanford.futuredata.macrobase.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

/**
 * Generic class for loading configs from a variety of sources (right now yaml).
 * Used for setting parameters to pipelines.
 */
public class PipelineConfig {
    private Map<String, Object> values;

    public PipelineConfig(Map<String, Object> values) {
        this.values = values;
    }

    public static PipelineConfig fromYamlFile(String fileName) throws Exception {
        BufferedReader r = new BufferedReader(new FileReader(fileName));
        Yaml yaml = new Yaml();
        Map<String, Object> conf = (Map<String, Object>) yaml.load(r);
        return new PipelineConfig(conf);
    }
    public static PipelineConfig fromJsonString(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(
                json,
                new TypeReference<Map<String, Object>>() {}
        );
        return new PipelineConfig(map);
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
