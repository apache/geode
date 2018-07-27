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
package org.apache.geode.security.templates;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Unit tests for {@link UsernamePrincipal}
 */
@Category({SecurityTest.class})
public class UsernamePrincipalTest {

  @Test
  public void isSerializable() throws Exception {
    assertThat(UsernamePrincipal.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void canBeSerialized() throws Exception {
    String name = "jsmith";
    UsernamePrincipal instance = new UsernamePrincipal(name);

    UsernamePrincipal cloned = (UsernamePrincipal) SerializationUtils.clone(instance);

    assertThat(cloned.getName()).isEqualTo(name);
  }
}
