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
package com.gemstone.gemfire.test.junit.categories;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.gemstone.gemfire.test.junit.rules.TestRunner;

@Category(UnitTest.class)
public class CategoryTest {

  private static boolean executedClassOneMethodNone; // 1
  private static boolean executedClassOneMethodTwo; // 2
  private static boolean executedClassTwoMethodTwo; // 3
  private static boolean executedClassNoneMethodOne; // 4
  private static boolean executedClassNoneMethodTwo; // 5
  private static boolean executedClassTwoMethodOne; // 6
  private static boolean executedClassOneMethodOne; // 7
  private static boolean executedClassOneAndTwoMethodNone; // 8
  private static boolean executedClassNoneMethodOneAndTwo; // 9

  @BeforeClass
  public static void beforeClass() throws Exception {
    executedClassOneMethodNone = false;
    executedClassOneMethodTwo = false;
    executedClassTwoMethodTwo = false;
    executedClassNoneMethodOne = false;
    executedClassNoneMethodTwo = false;
    executedClassTwoMethodOne = false;
    executedClassOneMethodOne = false;
    executedClassOneAndTwoMethodNone = false;
    executedClassNoneMethodOneAndTwo = false;
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Test
  public void allTestsWithCategoryOneShouldBeExecuted() {
    Result result = TestRunner.runTest(CategoryTestSuite.class);

    assertThat(result.wasSuccessful()).isTrue();
    assertThat(executedClassOneMethodNone).isTrue();
    assertThat(executedClassOneMethodTwo).isTrue();
    assertThat(executedClassTwoMethodTwo).isFalse();
    assertThat(executedClassNoneMethodOne).isTrue();
    assertThat(executedClassNoneMethodTwo).isFalse();
    assertThat(executedClassTwoMethodOne).isTrue();
    assertThat(executedClassOneMethodOne).isTrue();
    assertThat(executedClassOneAndTwoMethodNone).isTrue();
    assertThat(executedClassNoneMethodOneAndTwo).isTrue();
  }

  @Category(CategoryOne.class)
  public static class ClassOneMethodNone { // 1
    @Test
    public void test() {
      executedClassOneMethodNone = true;
    }
  }

  @Category(CategoryOne.class)
  public static class ClassOneMethodTwo { // 2
    @Category(CategoryTwo.class)
    @Test
    public void test() {
      executedClassOneMethodTwo = true;
    }
  }

  @Category(CategoryTwo.class)
  public static class ClassTwoMethodTwo { // 3
    @Category(CategoryTwo.class)
    @Test
    public void test() {
      executedClassTwoMethodTwo = true;
    }
  }

  public static class ClassNoneMethodOne { // 4
    @Category(CategoryOne.class)
    @Test
    public void test() {
      executedClassNoneMethodOne = true;
    }
  }

  public static class ClassNoneMethodTwo { // 5
    @Category(CategoryTwo.class)
    @Test
    public void test() {
      executedClassNoneMethodTwo = true;
    }
  }

  @Category(CategoryTwo.class)
  public static class ClassTwoMethodOne { // 6
    @Category(CategoryOne.class)
    @Test
    public void test() {
      executedClassTwoMethodOne = true;
    }
  }

  @Category(CategoryOne.class)
  public static class ClassOneMethodOne { // 7
    @Category(CategoryOne.class)
    @Test
    public void test() {
      executedClassOneMethodOne = true;
    }
  }

  @Category({ CategoryOne.class, CategoryTwo.class })
  public static class ClassOneAndTwoMethodNone { // 8
    @Test
    public void test() {
      executedClassOneAndTwoMethodNone = true;
    }
  }

  public static class ClassNoneMethodOneAndTwo { // 9
    @Category({ CategoryOne.class, CategoryTwo.class })
    @Test
    public void test() {
      executedClassNoneMethodOneAndTwo = true;
    }
  }

  @RunWith(Categories.class)
  @Categories.IncludeCategory(CategoryOne.class)
  @Suite.SuiteClasses({
          ClassOneMethodNone.class, // 1
          ClassOneMethodTwo.class, // 2
          ClassTwoMethodTwo.class, // 3
          ClassNoneMethodOne.class, // 4
          ClassNoneMethodTwo.class, // 5
          ClassTwoMethodOne.class, // 6
          ClassOneMethodOne.class, // 7
          ClassOneAndTwoMethodNone.class, // 8
          ClassNoneMethodOneAndTwo.class // 9
  })
  public static class CategoryTestSuite {
  }
}
