package macrobase.analysis.stats.kde;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import macrobase.conf.MacroBaseConf;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TKDEConfTest {
    @Test
    public void testParse() throws Exception {
        String content = new String(
                Files.readAllBytes(Paths.get("src/test/resources/conf/test_treekde.yaml"))
        );
        TKDEConf tkdeConf = TKDEConf.parseYAML(content);
        assertThat(tkdeConf.qSampleSize, is(20000));
    }

    @Test
    public void testLoad() throws Exception {
        TKDEConf tkdeConf = TKDEConf.load("src/test/resources/conf/test_treekde.yaml");
        assertThat(tkdeConf.qSampleSize, is(20000));
    }

    @Test
    public void testLoadInGlobalConfig() throws Exception {
        ConfigurationFactory<MacroBaseConf> cfFactory = new ConfigurationFactory<>(
                MacroBaseConf.class,
                null,
                Jackson.newObjectMapper(),
                "");
        MacroBaseConf conf = cfFactory.build(
                new File("src/test/resources/conf/test_treekde_in_global.yaml"));

        TKDEConf tConf = TKDEConf.parseYAML(conf.getSubConfString("tkdeConf"));
        assertThat(tConf.qSampleSize, is(20000));
        assertThat(tConf.gridSizes.size(), is(1));
    }
}
