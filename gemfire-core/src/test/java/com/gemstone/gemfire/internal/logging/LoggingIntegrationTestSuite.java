package com.gemstone.gemfire.internal.logging;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.gemstone.gemfire.internal.logging.log4j.FastLoggerIntegrationJUnitTest;
import com.gemstone.gemfire.internal.logging.log4j.FastLoggerWithDefaultConfigJUnitTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  DistributedSystemLogFileJUnitTest.class,
  LocatorLogFileJUnitTest.class,
  LogServiceIntegrationJUnitTest.class,
  MergeLogFilesJUnitTest.class,
  FastLoggerWithDefaultConfigJUnitTest.class,
  FastLoggerIntegrationJUnitTest.class,
})
public class LoggingIntegrationTestSuite {
}
