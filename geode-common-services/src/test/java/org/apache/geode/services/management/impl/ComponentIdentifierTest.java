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

package org.apache.geode.services.management.impl;


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ComponentIdentifierTest {

  @Test(expected = IllegalArgumentException.class)
  public void testForNullName() {
    new ComponentIdentifier(null);
  }

  @Test
  public void testForNullPath() {
    ComponentIdentifier componentIdentifier = new ComponentIdentifier("Name", null);
    assertThat(componentIdentifier.getComponentName()).isEqualTo("Name");
    assertThat(componentIdentifier.getPath().isPresent()).isFalse();
  }

  @Test
  public void testHappyCase() {
    ComponentIdentifier componentIdentifier = new ComponentIdentifier("Name");
    assertThat(componentIdentifier.getComponentName()).isEqualTo("Name");
    assertThat(componentIdentifier.getPath().isPresent()).isFalse();
  }

  @Test
  public void testHappyCase2() {
    ComponentIdentifier componentIdentifier = new ComponentIdentifier("Name", "Path");
    assertThat(componentIdentifier.getComponentName()).isEqualTo("Name");
    assertThat(componentIdentifier.getPath().get()).isEqualTo("Path");
  }

  @Test
  public void testEqual() {
    ComponentIdentifier componentIdentifier = new ComponentIdentifier("Name", "Path");
    ComponentIdentifier componentIdentifier2 = new ComponentIdentifier("Name", "Path2");
    assertThat(componentIdentifier.equals(componentIdentifier2)).isTrue();
  }

  @Test
  public void testEqual2() {
    ComponentIdentifier componentIdentifier = new ComponentIdentifier("Name", "Path");
    ComponentIdentifier componentIdentifier2 = new ComponentIdentifier("Name2", "Path");
    assertThat(componentIdentifier.equals(componentIdentifier2)).isFalse();
  }
}
