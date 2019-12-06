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

package org.apache.geode.util.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.junit.Test;

public class GeodeJsonMapperTest {
  @Test
  public void ignoreUnknownPropMapper() throws Exception {
    ObjectMapper mapper = GeodeJsonMapper.getMapperIgnoringUnknownProperties();
    String json = "{\"name\":\"Joe\"}";

    Employee employee = new Employee();
    employee.setName("Joe");
    assertThat(mapper.writeValueAsString(employee)).isEqualTo(json);

    String jsonWithUnknownProp = "{\"name\":\"Joe\",\"id\":\"test\"}";
    employee = mapper.readValue(jsonWithUnknownProp, Employee.class);
    assertThat(employee.getName()).isEqualTo("Joe");
    assertThat(employee.getTitle()).isNull();
  }

  @Test
  public void regularMapper() throws Exception {
    ObjectMapper mapper = GeodeJsonMapper.getMapper();
    String json = "{\"name\":\"Joe\"}";

    Employee employee = new Employee();
    employee.setName("Joe");
    assertThat(mapper.writeValueAsString(employee)).isEqualTo(json);

    String jsonWithUnknownProp = "{\"name\":\"Joe\",\"id\":\"test\"}";
    assertThatThrownBy(() -> mapper.readValue(jsonWithUnknownProp, Employee.class))
        .isInstanceOf(UnrecognizedPropertyException.class)
        .hasMessageContaining("Unrecognized field \"id\"");
  }

  private static class Employee {
    private String name;
    private String title;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getTitle() {
      return title;
    }

    public void setTitle(String title) {
      this.title = title;
    }
  }
}
