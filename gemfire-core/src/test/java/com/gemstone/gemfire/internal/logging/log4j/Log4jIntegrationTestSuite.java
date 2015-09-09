package com.gemstone.gemfire.internal.logging.log4j;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  FastLoggerWithDefaultConfigJUnitTest.class,
  FastLoggerIntegrationJUnitTest.class,
})
public class Log4jIntegrationTestSuite {
}
