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
package org.apache.geode.test.version;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TestVersionTest {

  @Test
  public void lessThan() {
    assertThat(TestVersion.valueOf("1.0.0").lessThan(TestVersion.valueOf("1.0.1"))).isTrue();

    assertThat(TestVersion.valueOf("1.0.0").lessThan(TestVersion.valueOf("1.0.0"))).isFalse();
    assertThat(TestVersion.valueOf("1.1.10").lessThan(TestVersion.valueOf("1.1.0"))).isFalse();
  }

  @Test
  public void testEquals() {
    assertThat(TestVersion.valueOf("1.0.0").equals(TestVersion.valueOf("1.0.0"))).isTrue();

    assertThat(TestVersion.valueOf("1.0.0").equals(TestVersion.valueOf("2.0.0"))).isFalse();
  }

  @Test
  public void greaterThan() {
    assertThat(TestVersion.valueOf("1.0.1").greaterThan(TestVersion.valueOf("1.0.0"))).isTrue();

    assertThat(TestVersion.valueOf("1.0.0").greaterThan(TestVersion.valueOf("1.0.0"))).isFalse();
    assertThat(TestVersion.valueOf("1.1.11").greaterThan(TestVersion.valueOf("1.11.1"))).isFalse();
  }

  @Test
  public void lessThanOrEqualTo() {
    assertThat(TestVersion.valueOf("1.0.0").lessThanOrEqualTo(TestVersion.valueOf("1.0.0")))
        .isTrue();
    assertThat(TestVersion.valueOf("1.0.0").lessThanOrEqualTo(TestVersion.valueOf("1.1.0")))
        .isTrue();

    assertThat(TestVersion.valueOf("12.0.1").lessThanOrEqualTo(TestVersion.valueOf("1.20.1")))
        .isFalse();
  }

  @Test
  public void greaterThanOrEqualTo() {
    assertThat(TestVersion.valueOf("1.0.0").greaterThanOrEqualTo(TestVersion.valueOf("1.0.0")))
        .isTrue();
    assertThat(TestVersion.valueOf("2.0.1").greaterThanOrEqualTo(TestVersion.valueOf("2.0.0")))
        .isTrue();

    assertThat(TestVersion.valueOf("2.0.0").greaterThanOrEqualTo(TestVersion.valueOf("20.0.1")))
        .isFalse();
  }
}
