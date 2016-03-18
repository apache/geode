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
package com.gemstone.gemfire.test.junit.rules.examples;

import static org.assertj.core.api.Assertions.*;

import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.gemfire.test.junit.rules.RetryRule;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Example usage of {@link RetryRule} with global scope.
 */
@Category(UnitTest.class)
public class RetryRuleExampleTest {

  private static int count = 0;

  @Rule
  public RetryRule retry = new RetryRule(2);

  @BeforeClass
  public static void beforeClass() {
    count = 0;
  }

  @Test
  public void unreliableTestWithRaceConditions() {
    count++;
    if (count < 2) {
      assertThat(count).isEqualTo(2); // doomed to fail
    }
  }
}
