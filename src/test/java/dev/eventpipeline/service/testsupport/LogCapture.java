package dev.eventpipeline.service.testsupport;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Attaches a Logback {@link ListAppender} to a given logger for the lifetime of a try-with-resources
 * block, so tests can assert on WARN/ERROR log lines without parsing console output.
 */
public final class LogCapture implements AutoCloseable {

    private final Logger logbackLogger;
    private final ListAppender<ILoggingEvent> appender;

    private LogCapture(Logger logbackLogger, ListAppender<ILoggingEvent> appender) {
        this.logbackLogger = logbackLogger;
        this.appender = appender;
    }

    public static LogCapture attach(Class<?> loggerClass) {
        Logger logbackLogger = (Logger) LoggerFactory.getLogger(loggerClass);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logbackLogger.addAppender(appender);
        return new LogCapture(logbackLogger, appender);
    }

    public List<ILoggingEvent> events() {
        return List.copyOf(appender.list);
    }

    @Override
    public void close() {
        logbackLogger.detachAppender(appender);
        appender.stop();
    }
}
