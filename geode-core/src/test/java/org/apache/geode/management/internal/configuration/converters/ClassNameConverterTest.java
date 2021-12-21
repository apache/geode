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

package org.apache.geode.management.internal.configuration.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.management.configuration.ClassName;

public class ClassNameConverterTest {
  private final ClassNameConverter converter = new ClassNameConverter();

  @Test
  public void convertNull() throws Exception {
    assertThat(converter.fromConfigObject(null)).isNull();
    assertThat(converter.fromXmlObject(null)).isNull();
  }

  @Test
  public void fromConfig() {
    Properties properties = new Properties();
    properties.put("field1", "value1");
    properties.put("field2", "value2");
    ClassName className = new ClassName("xyz", properties);
    DeclarableType declarableType = converter.fromConfigObject(className);
    assertThat(declarableType.getClassName()).isEqualTo("xyz");
    List<ParameterType> parameters = declarableType.getParameters();
    assertThat(parameters).extracting(p -> p.getName()).containsExactlyInAnyOrder("field1",
        "field2");
    assertThat(parameters).extracting(p -> p.getString()).containsExactlyInAnyOrder("value1",
        "value2");
  }

  @Test
  public void fromDeclarable() {
    DeclarableType type = new DeclarableType();
    type.setClassName("xyz");

    // this will get converted
    ParameterType param1 = new ParameterType();
    param1.setName("field1");
    param1.setString("value1");

    // this won't get converted
    ParameterType param2 = new ParameterType();
    param2.setName("field2");
    param2.setDeclarable(new DeclarableType());

    type.getParameters().add(param1);
    type.getParameters().add(param2);

    ClassName className = converter.fromXmlObject(type);
    assertThat(className.getClassName()).isEqualTo("xyz");
    assertThat(className.getInitProperties()).hasSize(1).containsOnlyKeys("field1");
  }
}
