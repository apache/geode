/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.test.junit.rules;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Rule;
import org.junit.Test;

public class RandomRuleTest {

  @Rule
  public RandomRule randomRule = new RandomRule();

  @Test
  public void isSerializable() {
    assertThat(randomRule).isInstanceOf(Serializable.class);
  }

  @Test
  public void serializes() {
    RandomRule clone = SerializationUtils.clone(randomRule);

    assertThat(clone.getSeed()).isEqualTo(randomRule.getSeed());
  }

  @Test
  public void iterableWithOneElementReturnsThatElement() {
    String value = randomRule.next(singleton("item"));

    assertThat(value).isEqualTo("item");
  }

  @Test
  public void nullNullIterableThrowsNullPointerException() {
    Iterable<String> input = null;

    Throwable thrown = catchThrowable(() -> randomRule.next(input));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void emptyIterableThrowsIllegalArgumentException() {
    Throwable thrown = catchThrowable(() -> randomRule.next(emptyList()));

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void iterableWithTwoElementsReturnsRandomElement() {
    String value = randomRule.next(asList("one", "two"));

    assertThat(value).isIn("one", "two");
  }

  @Test
  public void iterableWithManyElementsReturnsRandomElement() {
    String value = randomRule.next(asList("one", "two", "three"));

    assertThat(value).isIn("one", "two", "three");
  }

  @Test
  public void varArgsWithOneElementReturnsThatElement() {
    String value = randomRule.next("item");

    assertThat(value).isEqualTo("item");
  }

  @Test
  public void varArgsWithTwoElementsReturnsRandomElement() {
    String value = randomRule.next("one", "two");

    assertThat(value).isIn("one", "two");
  }

  @Test
  public void varArgsWithManyElementsReturnsRandomElement() {
    String value = randomRule.next("one", "two", "three");

    assertThat(value).isIn("one", "two", "three");
  }

  @Test
  public void nullVarArgThrowsNullPointerException() {
    String input = null;

    Throwable thrown = catchThrowable(() -> randomRule.next(input));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void zeroVarArgsThrowsIllegalArgumentException() {
    String[] input = new String[0];

    Throwable thrown = catchThrowable(() -> randomRule.next(input));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("At least one element is required");
  }
}
