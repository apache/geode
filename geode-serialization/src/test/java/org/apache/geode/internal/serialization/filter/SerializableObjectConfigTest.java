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
package org.apache.geode.internal.serialization.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class SerializableObjectConfigTest {

  @Test
  public void getSerializableObjectFilterIfEnabled_returnsFilter_ifValidateIsTrue() {
    SerializableObjectConfig config = spy(SerializableObjectConfig.class);
    when(config.getSerializableObjectFilter()).thenReturn("filter-string");
    when(config.getValidateSerializableObjects()).thenReturn(true);

    String result = config.getSerializableObjectFilterIfEnabled();

    assertThat(result).isEqualTo("filter-string");
  }

  @Test
  public void getSerializableObjectFilterIfEnabled_returnsNull_ifValidateIsFalse() {
    SerializableObjectConfig config = spy(SerializableObjectConfig.class);
    when(config.getSerializableObjectFilter()).thenReturn("filter-string");
    when(config.getValidateSerializableObjects()).thenReturn(false);

    String result = config.getSerializableObjectFilterIfEnabled();

    assertThat(result).isNull();
  }
}
