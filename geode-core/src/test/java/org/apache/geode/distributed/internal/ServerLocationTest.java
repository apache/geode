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
package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ServerLocationTest {

  @Test
  public void givenTwoObjectsWithSameHostAndPortwhenCompared_thenAreEquals() {
    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    final ServerLocation serverLocation2 = new ServerLocation("localhost", 1);

    assertThat(serverLocation1).isEqualTo(serverLocation2);
  }


  @Test
  public void givenTwoObjectsWithSamePortAndDifferentPortwhenCompared_thenAreNotEquals() {
    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    final ServerLocation serverLocation2 = new ServerLocation("localhost", 2);

    assertThat(serverLocation1).isNotEqualTo(serverLocation2);
  }

  @Test
  public void objectCreatedWithCopyConstructorwhenComparedToOrigin_isEqual() {
    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    final ServerLocation serverLocation2 = new ServerLocation(serverLocation1);

    assertThat(serverLocation1).isEqualTo(serverLocation2);
  }
}
