package com.gemstone.gemfire.test.golden;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  FailWithErrorInOutputJUnitTest.class,
  FailWithExtraLineInOutputJUnitTest.class,
  FailWithLineMissingFromEndOfOutputJUnitTest.class,
  FailWithLineMissingFromMiddleOfOutputJUnitTest.class,
  FailWithLoggerErrorInOutputJUnitTest.class,
  FailWithLoggerFatalInOutputJUnitTest.class,
  FailWithLoggerWarnInOutputJUnitTest.class,
  FailWithSevereInOutputJUnitTest.class,
  FailWithTimeoutOfWaitForOutputToMatchJUnitTest.class,
  FailWithWarningInOutputJUnitTest.class,
  PassJUnitTest.class,
  PassWithExpectedErrorJUnitTest.class,
  PassWithExpectedSevereJUnitTest.class,
  PassWithExpectedWarningJUnitTest.class,
})
/**
 * Suite of tests for the test.golden Golden Test framework classes.
 */
public class GoldenTestFrameworkTestSuite {
}
