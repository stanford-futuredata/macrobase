package edu.stanford.futuredata.macrobase;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingTest {
    @Test
    public void testSimpleLog() {
        Logger log = LoggerFactory.getLogger(LoggingTest.class);
        log.debug("Hello World");
    }
}
