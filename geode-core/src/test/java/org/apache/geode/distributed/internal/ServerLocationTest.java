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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class ServerLocationTest {

  /**
   * Make sure two ServerLocation objects on different hosts but with the same port are not equal
   *
   * <p>
   * Fix: LoadBalancing directs all traffic to a single cache server if all servers are started on
   * the same port
   */
  @Test
  public void serverLocationOnDifferentHostsShouldNotTestEqual() {
    ServerLocation serverLocation1 = new ServerLocation("host1", 777);
    ServerLocation serverLocation2 = new ServerLocation("host2", 777);

    assertThat(serverLocation1).isNotEqualTo(serverLocation2);
  }
}
