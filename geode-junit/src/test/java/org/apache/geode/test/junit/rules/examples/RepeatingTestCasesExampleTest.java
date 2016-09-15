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
package org.apache.geode.test.junit.rules.examples;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.Repeat;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.RepeatRule;

/**
 * The RepeatingTestCasesExampleTest class is a test suite of test cases testing the contract and functionality
 * of the JUnit {@literal @}Repeat annotation on a test suite class test case methods.
 *
 * @see org.junit.Test
 * @see org.apache.geode.test.junit.Repeat
 * @see org.apache.geode.test.junit.rules.RepeatRule
 */
@Category(UnitTest.class)
public class RepeatingTestCasesExampleTest {

  private static final AtomicInteger repeatOnceCounter = new AtomicInteger(0);
  private static final AtomicInteger repeatOnlyOnceCounter = new AtomicInteger(0);
  private static final AtomicInteger repeatTenTimesCounter = new AtomicInteger(0);
  private static final AtomicInteger repeatTwiceCounter = new AtomicInteger(0);

  @Rule
  public RepeatRule repeatRule = new RepeatRule();

  @BeforeClass
  public static void setupBeforeClass() {
    System.setProperty("tdd.example.test.case.two.repetitions", "2");
    repeatOnceCounter.set(0);
    repeatOnlyOnceCounter.set(0);
    repeatTenTimesCounter.set(0);
    repeatTwiceCounter.set(0);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    assertThat(repeatOnceCounter.get(), is(equalTo(1)));
    assertThat(repeatOnlyOnceCounter.get(), is(equalTo(1)));
    assertThat(repeatTenTimesCounter.get(), is(equalTo(10)));
    assertThat(repeatTwiceCounter.get(), is(equalTo(2)));
  }

  @Test
  @Repeat
  public void repeatOnce() {
    repeatOnceCounter.incrementAndGet();
    assertThat(repeatOnceCounter.get() <= 1, is(true));
  }

  @Test
  @Repeat(property = "tdd.example.test.case.with.non-existing.system.property")
  public void repeatOnlyOnce() {
    repeatOnlyOnceCounter.incrementAndGet();
    assertThat(repeatOnlyOnceCounter.get() <= 1, is(true));
  }

  @Test
  @Repeat(10)
  public void repeatTenTimes() {
    repeatTenTimesCounter.incrementAndGet();
    assertThat(repeatTenTimesCounter.get() <= 10, is(true));
  }

  @Test
  @Repeat(property = "tdd.example.test.case.two.repetitions")
  public void repeatTwiceCounter() {
    repeatTwiceCounter.incrementAndGet();
    assertThat(repeatTwiceCounter.get() <= 2, is(true));
  }
}
