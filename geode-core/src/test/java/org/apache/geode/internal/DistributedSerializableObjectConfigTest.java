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
package org.apache.geode.internal;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.internal.serialization.filter.SerializableObjectConfig;

public class DistributedSerializableObjectConfigTest {

  @Test
  public void getValidateSerializableObjects_returnsTrue_whenPropertyIsTrue() {
    Properties properties = spy(new Properties());
    SerializableObjectConfig config = new DistributedSerializableObjectConfig(properties);
    when(properties.getProperty(VALIDATE_SERIALIZABLE_OBJECTS)).thenReturn("true");

    boolean result = config.getValidateSerializableObjects();

    assertThat(result).isTrue();
  }

  @Test
  public void setValidateSerializableObjects_setsPropertyValue() {
    Properties properties = spy(new Properties());
    SerializableObjectConfig config = new DistributedSerializableObjectConfig(properties);

    config.setValidateSerializableObjects(true);

    assertThat(properties.getProperty(VALIDATE_SERIALIZABLE_OBJECTS)).isEqualTo("true");
  }

  @Test
  public void getSerializableObjectFilter_returnsPropertyValue() {
    Properties properties = spy(new Properties());
    SerializableObjectConfig config = new DistributedSerializableObjectConfig(properties);
    when(properties.getProperty(SERIALIZABLE_OBJECT_FILTER)).thenReturn("!*");

    String result = config.getSerializableObjectFilter();

    assertThat(result).isEqualTo("!*");
  }
}
