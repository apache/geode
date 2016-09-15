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
package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import org.apache.geode.test.junit.Repeat;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link RepeatRule}.
 */
@Category(UnitTest.class)
public class RepeatRuleTest {

  private static final String ASSERTION_ERROR_MESSAGE = "failing test";
  
  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsUnused() {
    Result result = TestRunner.runTest(FailingTestShouldFailOneTimeWhenRepeatIsUnused.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsUnused.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassOneTimeWhenRepeatIsUnused() {
    Result result = TestRunner.runTest(PassingTestShouldPassOneTimeWhenRepeatIsUnused.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassOneTimeWhenRepeatIsUnused.count).isEqualTo(1);
  }

  @Test
  public void zeroValueShouldThrowIllegalArgumentException() {
    Result result = TestRunner.runTest(ZeroValueShouldThrowIllegalArgumentException.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Repeat value must be a positive integer");
    assertThat(ZeroValueShouldThrowIllegalArgumentException.count).isEqualTo(0);
  }
  
  @Test
  public void negativeValueShouldThrowIllegalArgumentException() {
    Result result = TestRunner.runTest(NegativeValueShouldThrowIllegalArgumentException.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Repeat value must be a positive integer");
    assertThat(NegativeValueShouldThrowIllegalArgumentException.count).isEqualTo(0);
  }

  /**
   * Characterizes the behavior but is not a requirement for {@code RepeatRule}.
   */
  @Test
  public void passingTestShouldBeSkippedWhenRepeatIsZero() {
    Result result = TestRunner.runTest(PassingTestShouldBeSkippedWhenRepeatIsZero.class);

    assertThat(result.wasSuccessful()).isFalse();
    assertThat(PassingTestShouldBeSkippedWhenRepeatIsZero.count).isEqualTo(0);
  }

  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsOne() {
    Result result = TestRunner.runTest(FailingTestShouldFailOneTimeWhenRepeatIsOne.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsOne.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassOneTimeWhenRepeatIsOne() {
    Result result = TestRunner.runTest(PassingTestShouldPassOneTimeWhenRepeatIsOne.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassOneTimeWhenRepeatIsOne.count).isEqualTo(1);
  }

  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsTwo() {
    Result result = TestRunner.runTest(FailingTestShouldFailOneTimeWhenRepeatIsTwo.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsTwo.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassTwoTimesWhenRepeatIsTwo() {
    Result result = TestRunner.runTest(PassingTestShouldPassTwoTimesWhenRepeatIsTwo.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassTwoTimesWhenRepeatIsTwo.count).isEqualTo(2);
  }

  @Test
  public void failingTestShouldFailOneTimeWhenRepeatIsThree() {
    Result result = TestRunner.runTest(FailingTestShouldFailOneTimeWhenRepeatIsThree.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(FailingTestShouldFailOneTimeWhenRepeatIsThree.count).isEqualTo(1);
  }

  @Test
  public void passingTestShouldPassThreeTimesWhenRepeatIsThree() {
    Result result = TestRunner.runTest(PassingTestShouldPassThreeTimesWhenRepeatIsThree.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassingTestShouldPassThreeTimesWhenRepeatIsThree.count).isEqualTo(3);
  }

  /**
   * Used by test {@link #failingTestShouldFailOneTimeWhenRepeatIsUnused()}
   */
  public static class FailingTestShouldFailOneTimeWhenRepeatIsUnused {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  /**
   * Used by test {@link #passingTestShouldPassOneTimeWhenRepeatIsUnused()}
   */
  public static class PassingTestShouldPassOneTimeWhenRepeatIsUnused {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #zeroValueShouldThrowIllegalArgumentException()}
   */
  public static class ZeroValueShouldThrowIllegalArgumentException {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(0)
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #negativeValueShouldThrowIllegalArgumentException()}
   */
  public static class NegativeValueShouldThrowIllegalArgumentException {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(-1)
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #passingTestShouldBeSkippedWhenRepeatIsZero()}
   */
  public static class PassingTestShouldBeSkippedWhenRepeatIsZero {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(0)
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #failingTestShouldFailOneTimeWhenRepeatIsOne()}
   */
  public static class FailingTestShouldFailOneTimeWhenRepeatIsOne {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(1)
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  /**
   * Used by test {@link #passingTestShouldPassOneTimeWhenRepeatIsOne()}
   */
  public static class PassingTestShouldPassOneTimeWhenRepeatIsOne {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(1)
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #failingTestShouldFailOneTimeWhenRepeatIsTwo()}
   */
  public static class FailingTestShouldFailOneTimeWhenRepeatIsTwo {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(2)
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  /**
   * Used by test {@link #passingTestShouldPassTwoTimesWhenRepeatIsTwo()}
   */
  public static class PassingTestShouldPassTwoTimesWhenRepeatIsTwo {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(2)
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #failingTestShouldFailOneTimeWhenRepeatIsThree()}
   */
  public static class FailingTestShouldFailOneTimeWhenRepeatIsThree {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(3)
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  /**
   * Used by test {@link #passingTestShouldPassThreeTimesWhenRepeatIsThree()}
   */
  public static class PassingTestShouldPassThreeTimesWhenRepeatIsThree {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RepeatRule repeat = new RepeatRule();

    @Test
    @Repeat(3)
    public void doTest() throws Exception {
      count++;
    }
  }
}
