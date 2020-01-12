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

package org.apache.geode.cache.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Test;


public class DeclarableTypeTest {

  @Test
  public void declarableWithStringParam() {
    DeclarableType declarable = new DeclarableType("className");
    assertThat(declarable).isEqualTo(declarable);
    assertThat(declarable).isEqualTo(new DeclarableType("className"));
    assertThat(declarable).isNotEqualTo(new DeclarableType("anotherClassName"));

    ParameterType parameter = new ParameterType("name", "value");
    declarable.getParameters().add(parameter);

    DeclarableType d2 = new DeclarableType("className");
    assertThat(declarable).isNotEqualTo(d2);
    d2.getParameters().add(parameter);
    assertThat(declarable).isEqualTo(d2);

    // has one common parameter, but d2 has additional param
    d2.getParameters().add(new ParameterType("name1", "value1"));
    assertThat(declarable).isNotEqualTo(d2);

    // same size, but different param value
    DeclarableType d3 = new DeclarableType("className");
    d3.getParameters().add(new ParameterType("name", "value1"));
    assertThat(declarable).isNotEqualTo(d3);

    assertThat(declarable.toString()).isEqualTo("className{name:value}");
  }

  @Test
  public void declarableWithDeclarableParam() {
    DeclarableType declarable = new DeclarableType("className");
    ParameterType param = new ParameterType("param1");
    param.setDeclarable(new DeclarableType("anotherClassName", "{'k':'v'}"));
    declarable.getParameters().add(param);
    assertThat(declarable.toString()).isEqualTo("className{param1:anotherClassName{k:v}}");

    DeclarableType d2 = new DeclarableType("className");
    ParameterType p2 = new ParameterType("param1");
    p2.setDeclarable(new DeclarableType("anotherClassName", "{'k':'v'}"));
    d2.getParameters().add(p2);
    assertThat(declarable).isEqualTo(d2);

    DeclarableType d3 = new DeclarableType("className");
    ParameterType p3 = new ParameterType("param1");
    p3.setDeclarable(new DeclarableType("anotherClassName", "{'k':'v2'}"));
    d3.getParameters().add(p3);
    assertThat(declarable).isNotEqualTo(d3);
  }

  @Test
  public void emptyDeclarable() {
    assertThat(new DeclarableType(""))
        .isEqualTo(new DeclarableType("", (Properties) null))
        .isEqualTo(new DeclarableType("", ""))
        .isEqualTo(new DeclarableType("", "{}"))
        .isEqualTo(new DeclarableType(" ", "{}"))
        .isEqualTo(new DeclarableType(null, "{}"))
        .isEqualTo(DeclarableType.EMPTY);
  }
}
