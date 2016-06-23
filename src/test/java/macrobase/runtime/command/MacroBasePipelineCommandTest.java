package macrobase.runtime.command;

import junit.framework.TestCase;
import macrobase.conf.MacroBaseConf;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Test;

import java.util.HashMap;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

public class MacroBasePipelineCommandTest {
    @Test
    public void testPipelineRuns() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();

        conf.set(MacroBaseConf.PIPELINE_NAME, "macrobase.runtime.command.MacroBasePipelineCommandTest");
        MacroBasePipelineCommand cmd1 = new MacroBasePipelineCommand();
        cmd1.run(null, new Namespace(new HashMap<>()), conf);

        assertFalse(MacroBaseMockPipeline.initialized);
        TestCase.assertFalse(MacroBaseMockPipeline.ran);

        conf.set(MacroBaseConf.PIPELINE_NAME, "macrobase.runtime.command.MacroBaseMockPipeline");

        MacroBasePipelineCommand cmd2 = new MacroBasePipelineCommand();
        cmd2.run(null, new Namespace(new HashMap<>()), conf);

        assertTrue(MacroBaseMockPipeline.initialized);
        assertTrue(MacroBaseMockPipeline.ran);

        MacroBaseMockPipeline.initialized = false;
        MacroBaseMockPipeline.ran = false;

        MacroBasePipelineCommand cmd3 = new MacroBasePipelineCommand();
        cmd3.run(null, new Namespace(new HashMap<>()), conf);

        assertTrue(MacroBaseMockPipeline.initialized);
        assertTrue(MacroBaseMockPipeline.ran);
    }
}
