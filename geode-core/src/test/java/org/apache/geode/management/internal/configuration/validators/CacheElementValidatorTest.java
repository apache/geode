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

package org.apache.geode.management.internal.configuration.validators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.CacheElementOperation;

public class CacheElementValidatorTest {

  private CacheElementValidator validator;
  private Region config;

  @Before
  public void before() throws Exception {
    validator = new CacheElementValidator();
    config = new Region();
  }

  @Test
  public void blankName() throws Exception {
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "identifier is required.");

    assertThatThrownBy(() -> validator.validate(CacheElementOperation.DELETE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "identifier is required.");
  }

  @Test
  public void invalidGroup_cluster() throws Exception {
    config.setName("test");
    config.setGroup("cluster");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "'cluster' is a reserved group name");
  }

  @Test
  public void invalidGroup_comma() throws Exception {
    config.setName("test");
    config.setGroup("group1,group2");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Group name should not contain comma");
  }

  @Test
  public void multipleGroups() throws Exception {
    config.setName("name");
    config.addGroup("group1");
    config.addGroup("group2");
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Can only create Region in one group at a time.");
  }
}
