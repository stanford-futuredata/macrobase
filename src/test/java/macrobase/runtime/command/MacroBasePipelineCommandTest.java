package macrobase.runtime.command;

import macrobase.conf.MacroBaseConf;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Test;

import java.util.HashMap;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

public class MacroBasePipelineCommandTest {
    @Test
    public void testPipelineRuns() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();

        conf.set(MacroBaseConf.PIPELINE_NAME, "macrobase.runtime.command.MacroBaseMockPipeline");

        MacroBasePipelineCommand cmd = new MacroBasePipelineCommand();
        cmd.run(null, new Namespace(new HashMap<>()), conf);

        assertTrue(MacroBaseMockPipeline.initialized);
        assertTrue(MacroBaseMockPipeline.ran);
    }
}
