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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.HashMap;

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class ServerLocationAndMemberIdTest {

  @Test
  public void givenTwoObjectsWithSameHostAndPortAndId_whenCompared_thenAreEquals() {
    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();

    ServerLocationAndMemberId serverLocationAndMemberId1 =
        new ServerLocationAndMemberId(serverLocation1, uniqueId1);
    ServerLocationAndMemberId serverLocationAndMemberId2 =
        new ServerLocationAndMemberId(serverLocation1, uniqueId1);
    assertEquals(serverLocationAndMemberId1, serverLocationAndMemberId2);
  }

  @Test
  public void givenTwoObjectsWithSameHostAndPortButDifferentViewId_whenCompared_thenAreNotEquals() {

    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    InternalDistributedMember idmWithView1 = new InternalDistributedMember("localhost", 1);
    idmWithView1.setVmViewId(1);
    InternalDistributedMember idmWithView2 = new InternalDistributedMember("localhost", 1);
    idmWithView2.setVmViewId(2);

    ServerLocationAndMemberId serverLocationAndMemberId1 =
        new ServerLocationAndMemberId(serverLocation1, idmWithView1.getUniqueId());
    ServerLocationAndMemberId serverLocationAndMemberId2 =
        new ServerLocationAndMemberId(serverLocation1, idmWithView2.getUniqueId());

    assertNotEquals(serverLocationAndMemberId1, serverLocationAndMemberId2);
  }

  @Test
  public void givenTwoObjectsWithDifferentHostPortAndId_whenCompared_thenAreNotEquals() {
    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    final ServerLocation serverLocation2 = new ServerLocation("localhost", 2);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();

    ServerLocationAndMemberId serverLocationAndMemberId1 =
        new ServerLocationAndMemberId(serverLocation1, uniqueId1);
    ServerLocationAndMemberId serverLocationAndMemberId2 =
        new ServerLocationAndMemberId(serverLocation2, uniqueId2);

    assertNotEquals(serverLocationAndMemberId1, serverLocationAndMemberId2);
  }

  @Test
  public void givenTwoObjectsWithSameHostAndPortAndSameDistributedMemberId_whenCompared_thenAreEqual() {

    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    InternalDistributedMember idmWithView1 = new InternalDistributedMember("localhost", 1);
    idmWithView1.setVmViewId(1);
    InternalDistributedMember idmWithView2 = new InternalDistributedMember("localhost", 1);
    idmWithView2.setVmViewId(1);

    ServerLocationAndMemberId serverLocationAndMemberId1 =
        new ServerLocationAndMemberId(serverLocation1, idmWithView1.getUniqueId());
    ServerLocationAndMemberId serverLocationAndMemberId2 =
        new ServerLocationAndMemberId(serverLocation1, idmWithView2.getUniqueId());

    assertEquals(serverLocationAndMemberId1, serverLocationAndMemberId2);
  }

  @Test
  public void givenTwoObjectsWithSameHostAndPortAndSameDistributedMemberId_CannotBeAddedTwiceToHashMap() {

    final ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    InternalDistributedMember idmWithView1 = new InternalDistributedMember("localhost", 1);
    idmWithView1.setVmViewId(1);
    InternalDistributedMember idmWithView2 = new InternalDistributedMember("localhost", 1);
    idmWithView2.setVmViewId(1);

    ServerLocationAndMemberId serverLocationAndMemberId1 =
        new ServerLocationAndMemberId(serverLocation1, idmWithView1.getUniqueId());
    ServerLocationAndMemberId serverLocationAndMemberId2 =
        new ServerLocationAndMemberId(serverLocation1, idmWithView2.getUniqueId());

    HashMap map = new HashMap<>();
    map.put(serverLocationAndMemberId1, new Integer(1));
    Integer i = (Integer) map.get(serverLocationAndMemberId2);
    assertNotEquals(null, i);
    assertEquals(new Integer(1), i);

    map.put(serverLocationAndMemberId2, new Integer(2));
    i = (Integer) map.get(serverLocationAndMemberId1);
    assertNotEquals(null, i);
    assertEquals(new Integer(2), i);

    assertEquals(serverLocationAndMemberId1, serverLocationAndMemberId2);
  }
}
