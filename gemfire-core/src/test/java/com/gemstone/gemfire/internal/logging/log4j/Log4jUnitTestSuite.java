package com.gemstone.gemfire.internal.logging.log4j;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  AlertAppenderJUnitTest.class,
  ConfigLocatorJUnitTest.class,
  FastLoggerJUnitTest.class,
  FastLoggerWithDefaultConfigJUnitTest.class,
  LocalizedMessageJUnitTest.class,
  LogWriterAppenderJUnitTest.class,
})
public class Log4jUnitTestSuite {
}
