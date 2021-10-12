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
package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.data.RedisProperties.getIntegerSystemProperty;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;


public class RedisPropertiesTest {
  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenNotSet() {
    assertThat(getIntegerSystemProperty("prop.name", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToEmptyString() {
    System.setProperty("prop.name0", "");
    assertThat(getIntegerSystemProperty("prop.name0", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToNonIntegerString() {
    System.setProperty("prop.name0", "nonintegervalue");
    assertThat(getIntegerSystemProperty("prop.name0", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToIntegerOutOfRange() {
    System.setProperty("prop.name1", "-5");
    assertThat(getIntegerSystemProperty("prop.name1", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSetValue_whenSetToIntegerInRange() {
    System.setProperty("prop.name2", "10");
    assertThat(getIntegerSystemProperty("prop.name2", 5, 0)).isEqualTo(10);
  }
}
