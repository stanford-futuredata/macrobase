package edu.stanford.futuredata.macrobase.logging;

import com.microsoft.applications.telemetry.EventProperties;
import com.microsoft.applications.telemetry.LogManager;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * Uses the Log4J extensibility framework to send Log4J logs to Aria.
 */
public class AriaLog4jAppender extends AppenderSkeleton {

    /**
     * Default constructor.
     */
    public AriaLog4jAppender() {
    }


    @Override
    public void activateOptions() {
        super.activateOptions();
    }


    /**
     * Override append and send the event to Aria.
     * @param loggingEvent
     */
    @Override
    protected void append(LoggingEvent loggingEvent) {

        // Create an Aria event for the Log4J logging event.

        EventProperties props = new EventProperties();
        props.setName("Log4jTrace");
        props.setProperty("Trace.Level", loggingEvent.getLevel().toString());
        props.setProperty("Trace.Message", loggingEvent.getRenderedMessage());
        props.setProperty("Trace.ClassName", loggingEvent.getFQNOfLoggerClass());
        props.setProperty("Trace.Location", loggingEvent.getLocationInformation().fullInfo);
        props.setProperty("Trace.LoggerName", loggingEvent.getLoggerName());

        // Log it to the default Aria logger. It will be sent by a background thread.
        LogManager.getLogger().logEvent(props);
    }


    @Override
    public void close() {
        // do nothing.
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
