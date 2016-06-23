package macrobase.runtime;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import org.hibernate.validator.internal.engine.ValidatorImpl;
import org.junit.Test;

import javax.validation.Validation;
import java.util.ArrayList;
import java.util.List;


public class MacroBaseServerTest {
    @Test
    public void simpleTest() throws Exception {
        MacroBaseServer server = new MacroBaseServer();
        server.getName();
        server.initialize(new Bootstrap<>(server));

        Environment env = new Environment("mbenv",
                                          new ObjectMapper(),
                                          Validation.buildDefaultValidatorFactory().getValidator(),
                                          new MetricRegistry(),
                                          MacroBaseServerTest.class.getClassLoader());
        server.run(new MacroBaseConf(), env);
    }
}
