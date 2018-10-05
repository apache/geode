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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

public class AbstractConfigTest {

  private ConfigSource source;
  private AbstractConfig abstractConfig;

  private String stringArrayAttributeName;

  @Before
  public void setUp() {
    source = mock(ConfigSource.class);
    abstractConfig = spy(AbstractConfig.class);

    stringArrayAttributeName = "stringArray";

    when(abstractConfig.getAttributeType(stringArrayAttributeName)).thenReturn(String[].class);
  }

  @Test
  public void toStringCanBeMocked() {
    when(abstractConfig.toString()).thenReturn("STRING");

    assertThat(abstractConfig.toString()).isEqualTo("STRING");
  }

  @Test
  public void setAttributeForStringArrayTypeWithEmpty() {
    abstractConfig.setAttribute(stringArrayAttributeName, "", source);

    verify(abstractConfig).setAttributeObject(stringArrayAttributeName, new String[] {""}, source);
  }

  @Test
  public void setAttributeForStringArrayTypeWithNull() {
    Throwable thrown =
        catchThrowable(() -> abstractConfig.setAttribute(stringArrayAttributeName, null, source));
    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void setAttributeForStringArrayTypeWithNoCommas() {
    abstractConfig.setAttribute(stringArrayAttributeName, "value", source);

    verify(abstractConfig).setAttributeObject(stringArrayAttributeName, new String[] {"value"},
        source);
  }

  @Test
  public void setAttributeForStringArrayTypeWithNestedComma() {
    abstractConfig.setAttribute(stringArrayAttributeName, "value1,value2", source);

    verify(abstractConfig).setAttributeObject(stringArrayAttributeName,
        new String[] {"value1", "value2"}, source);
  }

  @Test
  public void setAttributeForStringArrayTypeStartingWithComma() {
    abstractConfig.setAttribute(stringArrayAttributeName, ",value", source);

    verify(abstractConfig).setAttributeObject(stringArrayAttributeName, new String[] {"", "value"},
        source);
  }

  @Test
  public void setAttributeForStringArrayTypeEndingWithComma() {
    abstractConfig.setAttribute(stringArrayAttributeName, "value,", source);

    verify(abstractConfig).setAttributeObject(stringArrayAttributeName, new String[] {"value"},
        source);
  }
}
