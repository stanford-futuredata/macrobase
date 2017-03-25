package macrobase.conf;

import com.google.common.collect.Lists;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.DiskCachingIngester;
import macrobase.ingest.MySQLIngester;
import macrobase.ingest.PostgresIngester;
import org.junit.*;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

public class MacroBaseConfTest {
    @Test
    public void getSetTest() throws ConfigurationException {
        MacroBaseConf conf = new MacroBaseConf();

        conf.set("bool", true);
        assertTrue(conf.isSet("bool"));
        assertFalse(conf.isSet("loob"));
        assertEquals(true, conf.getBoolean("bool"));

        conf.set("int", 1);
        assertTrue(conf.isSet("int"));
        assertFalse(conf.isSet("tni"));
        assertEquals((Integer) 1, conf.getInt("int"));

        conf.set("long", 1);
        assertTrue(conf.isSet("long"));
        assertFalse(conf.isSet("gnol"));
        assertEquals((Long) 1L, conf.getLong("long"));

        conf.set("double", 0.01);
        assertTrue(conf.isSet("double"));
        assertFalse(conf.isSet("elbuod"));
        assertEquals((Double) 0.01, conf.getDouble("double"));

        conf.set("string", "test");
        assertTrue(conf.isSet("string"));
        assertFalse(conf.isSet("gnirts"));
        assertEquals("test", conf.getString("string"));

        List<String> testList = Lists.newArrayList("A", "B", "C");
        conf.set("stringList", testList);
        assertTrue(conf.isSet("stringList"));
        assertFalse(conf.isSet("tsiLgnirts"));
        assertArrayEquals(testList.toArray(), conf.getStringList("stringList").toArray());
    }

    @Test
    public void defaultGetterTest() throws ConfigurationException {
        MacroBaseConf conf = new MacroBaseConf();
        assertEquals(true, conf.getBoolean("bool", true));
        assertEquals((Integer) 1, conf.getInt("int", 1));
        assertEquals((Long) 1L, conf.getLong("long", 1L));
        assertEquals((Double) 0.01, conf.getDouble("double", 0.01));
        assertEquals("test", conf.getString("string", "test"));
        List<String> testList = Lists.newArrayList("A", "B", "C");
        assertArrayEquals(testList.toArray(), conf.getStringList("testList", testList).toArray());
    }

    @Test(expected = MissingParameterException.class)
    public void missingBooleanTest() throws ConfigurationException {
        (new MacroBaseConf()).getBoolean("dud");
    }

    @Test(expected = MissingParameterException.class)
    public void missingIntegerTest() throws ConfigurationException {
        (new MacroBaseConf()).getInt("dud");
    }

    @Test(expected = MissingParameterException.class)
    public void missingDoubleTest() throws ConfigurationException {
        (new MacroBaseConf()).getDouble("dud");
    }

    @Test(expected = MissingParameterException.class)
    public void missingLongTest() throws ConfigurationException {
        (new MacroBaseConf()).getLong("dud");
    }

    @Test(expected = MissingParameterException.class)
    public void missingStringTest() throws ConfigurationException {
        (new MacroBaseConf()).getString("dud");
    }

    @Test(expected = MissingParameterException.class)
    public void missingStringListTest() throws ConfigurationException {
        (new MacroBaseConf()).getStringList("dud");
    }

    @Test
    public void testParsing() throws Exception {
        ConfigurationFactory<MacroBaseConf> cfFactory = new ConfigurationFactory<>(MacroBaseConf.class,
                                                                                   null,
                                                                                   Jackson.newObjectMapper(),
                                                                                   "");
        MacroBaseConf conf = cfFactory.build(new File("src/test/resources/conf/simple.yaml"));


        assertEquals((Double) 0.1, conf.getDouble("this.is.a.double"));
        assertEquals((Integer) 100, conf.getInt("this.is.an.integer"));
        assertEquals((Long) 10000000000000L, conf.getLong("this.is.a.long"));
        assertEquals("Test", conf.getString("this.is.a.string"));

        List<String> stringList = Lists.newArrayList("T1", "T2", "T3", "T4");
        assertArrayEquals(stringList.toArray(), conf.getStringList("this.is.a.stringList").toArray());
        assertArrayEquals(stringList.toArray(), conf.getStringList("this.is.a.stringList.without.spaces").toArray());
        assertArrayEquals(stringList.toArray(), conf.getStringList("this.is.a.stringList.with.mixed.spaces").toArray());
    }

    @Test
    public void testIngester() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();

        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("testattr"));
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("lm1"));
        conf.set(MacroBaseConf.BASE_QUERY, Lists.newArrayList("basequery"));
        conf.set(MacroBaseConf.DB_CACHE_DIR, "cachedir");

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER);
        assertTrue(conf.constructIngester() instanceof CSVIngester);

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.MYSQL_LOADER);
        assertTrue(conf.constructIngester() instanceof MySQLIngester);

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CACHING_MYSQL_LOADER);
        assertTrue(conf.constructIngester() instanceof DiskCachingIngester);

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.POSTGRES_LOADER);
        assertTrue(conf.constructIngester() instanceof PostgresIngester);

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CACHING_POSTGRES_LOADER);
        assertTrue(conf.constructIngester() instanceof DiskCachingIngester);

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, "macrobase.conf.MockIngester");
        assertTrue(conf.constructIngester() instanceof MockIngester);
    }
}
