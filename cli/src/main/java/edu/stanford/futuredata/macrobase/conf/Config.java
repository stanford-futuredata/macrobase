package edu.stanford.futuredata.macrobase.conf;

import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

/**
 * Class to wrap the key-val map returned by parsing a yaml config file
 */
public class Config {
    private Map<String, Object> values;

    public Config(Map<String, Object> values) {
        this.values = values;
    }

    public static Config loadFromYaml(String fileName) throws Exception {
        BufferedReader r = new BufferedReader(new FileReader(fileName));
        Yaml yaml = new Yaml();
        Map<String, Object> conf = (Map<String, Object>) yaml.load(r);
        return new Config(conf);
    }

    @SuppressWarnings("unchecked")
    public <T> T getAs(String key) {
        return (T)values.get(key);
    }

    public Object get(String key) {
        return values.get(key);
    }

    public Map<String, Object> getValues() {
        return values;
    }
}
