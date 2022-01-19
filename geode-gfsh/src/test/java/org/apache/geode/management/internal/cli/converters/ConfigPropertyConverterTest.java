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
package org.apache.geode.management.internal.cli.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.JndiBindingsType;


public class ConfigPropertyConverterTest {

  private ConfigPropertyConverter converter;

  @Before
  public void setUp() throws Exception {
    converter = new ConfigPropertyConverter();
  }

  @Test
  public void validJson() {
    JndiBindingsType.JndiBinding.ConfigProperty configProperty =
        converter.convertFromText("{'name':'name','type':'type','value':'value'}", null, null);
    assertThat(configProperty.getName()).isEqualTo("name");
    assertThat(configProperty.getType()).isEqualTo("type");
    assertThat(configProperty.getValue()).isEqualTo("value");
  }

  @Test
  public void invalidJson() {
    assertThatThrownBy(() -> converter.convertFromText(
        "{'name':'name','type':'type','value':'value','another':'another'}", null, null))
            .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void invalidWhenEmptyString() {
    assertThatThrownBy(() -> converter.convertFromText("", null, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validWhenTypeMissing() {
    JndiBindingsType.JndiBinding.ConfigProperty configProperty =
        converter.convertFromText("{'name':'name','value':'value'}", null, null);
    assertThat(configProperty.getName()).isEqualTo("name");
    assertThat(configProperty.getType()).isNull();
    assertThat(configProperty.getValue()).isEqualTo("value");
  }

  @Test
  public void inValidWhenTypo() {
    assertThatThrownBy(() -> converter
        .convertFromText("{'name':'name','typo':'type','value':'value'}", null, null))
            .isInstanceOf(IllegalArgumentException.class);
  }
}
