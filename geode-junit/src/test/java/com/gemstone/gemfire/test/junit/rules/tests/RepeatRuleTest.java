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
package com.gemstone.gemfire.test.junit.rules.tests;

import static com.gemstone.gemfire.test.junit.rules.tests.TestRunner.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.gemstone.gemfire.test.junit.Repeat;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.gemfire.test.junit.rules.RepeatRule;

/**
 * Unit tests for Repeat JUnit Rule.
 * 
 */
@Category(UnitTest.class)
public class RepeatRuleTest {

  private static final String ASSERTION_ERROR_MESSAGE = "failing test";
  
  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsUnused() {
    Result result = runTest(FailingTestShouldFailOneTimeWhenRepeatIsUnused.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsUnused.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassOneTimeWhenRepeatIsUnused() {
    Result result = runTest(PassingTestShouldPassOneTimeWhenRepeatIsUnused.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassOneTimeWhenRepeatIsUnused.count).isEqualTo(1);
  }

  @Test
  public void zeroValueShouldThrowIllegalArgumentException() {
    Result result = runTest(ZeroValueShouldThrowIllegalArgumentException.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Repeat value must be a positive integer");
    assertThat(ZeroValueShouldThrowIllegalArgumentException.count).isEqualTo(0);
  }
  
  @Test
  public void negativeValueShouldThrowIllegalArgumentException() {
    Result result = runTest(NegativeValueShouldThrowIllegalArgumentException.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Repeat value must be a positive integer");
    assertThat(NegativeValueShouldThrowIllegalArgumentException.count).isEqualTo(0);
  }

  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsOne() {
    Result result = runTest(FailingTestShouldFailOneTimeWhenRepeatIsOne.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsOne.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassOneTimeWhenRepeatIsOne() {
    Result result = runTest(PassingTestShouldPassOneTimeWhenRepeatIsOne.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassOneTimeWhenRepeatIsOne.count).isEqualTo(1);
  }

  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsTwo() {
    Result result = runTest(FailingTestShouldFailOneTimeWhenRepeatIsTwo.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsTwo.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassTwoTimesWhenRepeatIsTwo() {
    Result result = runTest(PassingTestShouldPassTwoTimesWhenRepeatIsTwo.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassTwoTimesWhenRepeatIsTwo.count).isEqualTo(2);
  }

  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsThree() {
    Result result = runTest(FailingTestShouldFailOneTimeWhenRepeatIsThree.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsThree.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassThreeTimesWhenRepeatIsThree() {
    Result result = runTest(PassingTestShouldPassThreeTimesWhenRepeatIsThree.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassThreeTimesWhenRepeatIsThree.count).isEqualTo(3);
  }

  public static class FailingTestShouldFailOneTimeWhenRepeatIsUnused {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  public static class PassingTestShouldPassOneTimeWhenRepeatIsUnused {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    public void doTest() throws Exception {
      count++;
    }
  }

  public static class ZeroValueShouldThrowIllegalArgumentException {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(0)
    public void doTest() throws Exception {
      count++;
    }
  }

  public static class NegativeValueShouldThrowIllegalArgumentException {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(-1)
    public void doTest() throws Exception {
      count++;
    }
  }

  public static class PassingTestShouldBeSkippedWhenRepeatIsZero {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(0)
    public void doTest() throws Exception {
      count++;
    }
  }
  
  public static class FailingTestShouldFailOneTimeWhenRepeatIsOne {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(1)
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  public static class PassingTestShouldPassOneTimeWhenRepeatIsOne {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(1)
    public void doTest() throws Exception {
      count++;
    }
  }

  public static class FailingTestShouldFailOneTimeWhenRepeatIsTwo {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(2)
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  public static class PassingTestShouldPassTwoTimesWhenRepeatIsTwo {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(2)
    public void doTest() throws Exception {
      count++;
    }
  }

  public static class FailingTestShouldFailOneTimeWhenRepeatIsThree {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(3)
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  public static class PassingTestShouldPassThreeTimesWhenRepeatIsThree {
    protected static int count = 0;
    
    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(3)
    public void doTest() throws Exception {
      count++;
    }
  }
}
