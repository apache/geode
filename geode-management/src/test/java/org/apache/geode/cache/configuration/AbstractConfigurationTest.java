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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class AbstractConfigurationTest {

  private AbstractConfiguration<?> element;

  private static ObjectMapper mapper;
  private String json;

  @BeforeClass
  public static void beforeClass() {
    mapper = GeodeJsonMapper.getMapper();
  }

  @Before
  public void before() throws Exception {
    element = new Pdx();
  }

  @Test
  public void plainPdxConfig() throws Exception {
    assertThat(element.getGroup()).isNull();
    json = mapper.writeValueAsString(element);
    System.out.println(json);
    assertThat(json).doesNotContain("group").doesNotContain("configGroup");
  }

  @Test
  public void isCluster() {
    assertThat(AbstractConfiguration.isCluster("foo")).isFalse();
    assertThat(AbstractConfiguration.isCluster(null)).isTrue();
    assertThat(AbstractConfiguration.isCluster("")).isTrue();
    assertThat(AbstractConfiguration.isCluster(AbstractConfiguration.CLUSTER)).isTrue();
    assertThat(AbstractConfiguration.isCluster(AbstractConfiguration.CLUSTER.toLowerCase()))
        .isTrue();
    assertThat(AbstractConfiguration.isCluster(AbstractConfiguration.CLUSTER.toUpperCase()))
        .isTrue();
  }
}
