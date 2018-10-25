/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

public class DataSourceFactoryTest {

  private final Map<String, String> inputs = new HashMap<>();

  @Test
  public void creatPoolPropertiesWithNullInputReturnsEmptyOutput() {
    Properties output = DataSourceFactory.createPoolProperties(null);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithEmptyInputReturnsEmptyOutput() {
    Properties output = DataSourceFactory.createPoolProperties(inputs);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithNullValueInputReturnsEmptyOutput() {
    inputs.put("name", null);

    Properties output = DataSourceFactory.createPoolProperties(inputs);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithEmptyStringValueInputReturnsEmptyOutput() {
    inputs.put("name", "");

    Properties output = DataSourceFactory.createPoolProperties(inputs);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithOneInputReturnsOneOutput() {
    inputs.put("name", "value");

    Properties output = DataSourceFactory.createPoolProperties(inputs);

    assertThat(output.size()).isEqualTo(1);
    assertThat(output.getProperty("name")).isEqualTo("value");
  }

  @Test
  public void creatPoolPropertiesWithIgnoredInputKeysReturnsEmptyOutput() {
    inputs.put("type", "value");
    inputs.put("jndi-name", "value");
    inputs.put("transaction-type", "value");
    inputs.put("conn-pooled-datasource-class", "value");
    inputs.put("managed-conn-factory-class", "value");
    inputs.put("xa-datasource-class", "value");

    Properties output = DataSourceFactory.createPoolProperties(inputs);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithIgnoredAndValidInputsReturnsValidOutputs() {
    inputs.put("name1", "");
    inputs.put("name2", null);
    inputs.put("type", "value");
    inputs.put("jndi-name", "value");
    inputs.put("transaction-type", "value");
    inputs.put("conn-pooled-datasource-class", "value");
    inputs.put("managed-conn-factory-class", "value");
    inputs.put("xa-datasource-class", "value");
    inputs.put("validname1", "value1");
    inputs.put("validname2", "value2");

    Properties output = DataSourceFactory.createPoolProperties(inputs);

    assertThat(output.size()).isEqualTo(2);
    assertThat(output.getProperty("validname1")).isEqualTo("value1");
    assertThat(output.getProperty("validname2")).isEqualTo("value2");
  }

}
