package org.apache.geode.test.junit.rules;

import java.io.Serializable;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

/**
 * A rule that causes a test to be ignored if it is run on windows.
 *
 * This is best used a as a ClassRule to make sure it runs first. If you have
 * other class rules, us a {@link RuleChain} to make sure this runs first.
 */
public class IgnoreOnWindowsRule extends ExternalResource implements Serializable {
  @Override
  protected void before() throws Throwable {
    Assume.assumeFalse(SystemUtils.IS_OS_WINDOWS);
  }
}
