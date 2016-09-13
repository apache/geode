/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.test.junit.rules;

import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.hamcrest.Matcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Expect an Exception within a specified timeout.
 */
public class ExpectedTimeoutRule implements TestRule {

  /**
   * @return a Rule that expects no timeout (identical to behavior without this Rule)
   */
  public static ExpectedTimeoutRule none() {
    return new ExpectedTimeoutRule();
  }
  
  private ExpectedException delegate;
  private boolean expectsThrowable;
  private long minDuration;
  private long maxDuration;
  private TimeUnit timeUnit;
  
  private ExpectedTimeoutRule() {
    this.delegate = ExpectedException.none();
  }

  public ExpectedTimeoutRule expectMinimumDuration(final long minDuration) {
    this.minDuration = minDuration;
    return this;
  }
  public ExpectedTimeoutRule expectMaximumDuration(final long maxDuration) {
    this.maxDuration = maxDuration;
    return this;
  }
  public ExpectedTimeoutRule expectTimeUnit(final TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
    return this;
  }

  /**
   * Adds {@code matcher} to the list of requirements for any thrown exception.
   */
  public void expect(final Matcher<?> matcher) {
    this.delegate.expect(matcher);
  }

  /**
   * Adds to the list of requirements for any thrown exception that it should
   * be an instance of {@code type}.
   */
  public void expect(final Class<? extends Throwable> type) {
    this.delegate.expect(type);
    this.expectsThrowable = true;
  }

  /**
   * Adds to the list of requirements for any thrown exception that it should
   * <em>contain</em> string {@code substring}
   */
  public void expectMessage(final String substring) {
    this.delegate.expectMessage(substring);
  }

  /**
   * Adds {@code matcher} to the list of requirements for the message returned
   * from any thrown exception.
   */
  public void expectMessage(final Matcher<String> matcher) {
    this.delegate.expectMessage(matcher);
  }

  /**
   * Adds {@code matcher} to the list of requirements for the cause of
   * any thrown exception.
   */
  public void expectCause(final Matcher<? extends Throwable> expectedCause) {
    this.delegate.expectCause(expectedCause);
  }

  /**
   * Returns true if a timeout is expected.
   */
  protected boolean expectsTimeout() {
    return this.minDuration > 0 || this.maxDuration > 0;
  }

  /**
   * Returns true if a Throwable is expected.
   */
  protected boolean expectsThrowable() {
    return this.expectsThrowable;
  }
  
  @Override
  public Statement apply(final Statement base, final Description description) {
    Statement next = this.delegate.apply(base, description);
    return new ExpectedTimeoutStatement(next);
  }
  
  private void handleTime(final Long duration) {
    if (expectsTimeout()) {
      assertThat(this.timeUnit.convert(duration, TimeUnit.NANOSECONDS), new TimeMatcher(this.timeUnit, this.minDuration, this.maxDuration));
    }
  }
  
  private static class TimeMatcher extends org.hamcrest.TypeSafeMatcher<Long> {
    
    private final TimeUnit timeUnit;
    private final long minDuration;
    private final long maxDuration;
 
    public TimeMatcher(final TimeUnit timeUnit, final long minDuration, final long maxDuration) {
      this.timeUnit = timeUnit;
      this.minDuration = minDuration;
      this.maxDuration = maxDuration;
    }
 
    @Override
    public boolean matchesSafely(final Long duration) {
      return duration >= this.minDuration && duration <= this.maxDuration;
    }

    @Override
    public void describeTo(final org.hamcrest.Description description) {
      description.appendText("expects duration to be greater than or equal to ")
          .appendValue(this.minDuration)
          .appendText(" and less than or equal to ")
          .appendValue(this.maxDuration)
          .appendText(" ")
          .appendValue(this.timeUnit);
    }
  }
  
  private class ExpectedTimeoutStatement extends Statement {
    private final Statement next;

    public ExpectedTimeoutStatement(final Statement base) {
      this.next = base;
    }

    @Override
    public void evaluate() throws Throwable {
      long start = System.nanoTime();
      this.next.evaluate();
      handleTime(System.nanoTime() - start);
    }
  }
}
