package com.gemstone.gemfire.internal.logging;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  LogWriterImplJUnitTest.class,
  SortLogFileJUnitTest.class
})
public class LoggingUnitTestSuite {
}
