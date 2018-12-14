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

package org.apache.geode.cache.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.configuration.RegionAttributesType.ExpirationAttributesType;

public class RegionAttributesTypeTest {

  @Test
  public void no_arg_constructor() throws Exception {
    ExpirationAttributesType type = new ExpirationAttributesType();
    assertThat(type.getTimeout()).isNull();
    assertThat(type.getAction()).isNull();
    assertThat(type.getCustomExpiry()).isNull();
    assertThat(type.hasCustomExpiry()).isFalse();
    assertThat(type.hasTimoutOrAction()).isFalse();
  }

  @Test
  public void some_args_constructor() throws Exception {
    ExpirationAttributesType type = new ExpirationAttributesType(null, ExpirationAction.INVALIDATE, null, null);
    assertThat(type.getTimeout()).isNull();
    assertThat(type.getAction()).isEqualTo("invalidate");
    assertThat(type.getCustomExpiry()).isNull();
    assertThat(type.hasCustomExpiry()).isFalse();
    assertThat(type.hasTimoutOrAction()).isTrue();

    type = new ExpirationAttributesType(10, null, "foo", null);
    assertThat(type.getTimeout()).isEqualTo("10");
    assertThat(type.getAction()).isNull();
    assertThat(type.getCustomExpiry()).isEqualTo(new DeclarableType("foo"));
    assertThat(type.hasCustomExpiry()).isTrue();
    assertThat(type.hasTimoutOrAction()).isTrue();
  }
}