package macrobase.runtime;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.conf.MacroBaseConf;
import org.junit.Test;

import javax.validation.Validation;


public class MacroBaseApplicationTest {
    @Test
    public void simpleTest() throws Exception {
        MacroBaseApplication server = new MacroBaseApplication();
        server.getName();
        server.initialize(new Bootstrap<>(server));

        Environment env = new Environment("mbenv",
                                          new ObjectMapper(),
                                          Validation.buildDefaultValidatorFactory().getValidator(),
                                          new MetricRegistry(),
                                          MacroBaseApplicationTest.class.getClassLoader());
        server.run(new MacroBaseConf(), env);
    }
}
