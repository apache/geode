package com.gemstone.gemfire.internal.logging;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.gemstone.gemfire.internal.logging.log4j.AlertAppenderJUnitTest;
import com.gemstone.gemfire.internal.logging.log4j.ConfigLocatorJUnitTest;
import com.gemstone.gemfire.internal.logging.log4j.FastLoggerJUnitTest;
import com.gemstone.gemfire.internal.logging.log4j.FastLoggerWithDefaultConfigJUnitTest;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessageJUnitTest;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppenderJUnitTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  LogServiceJUnitTest.class,
  LogWriterImplJUnitTest.class,
  SortLogFileJUnitTest.class,
  AlertAppenderJUnitTest.class,
  ConfigLocatorJUnitTest.class,
  FastLoggerJUnitTest.class,
  FastLoggerWithDefaultConfigJUnitTest.class,
  LocalizedMessageJUnitTest.class,
  LogWriterAppenderJUnitTest.class,
})
public class LoggingUnitTestSuite {
}
