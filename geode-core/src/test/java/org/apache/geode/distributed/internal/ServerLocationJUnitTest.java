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

import org.junit.Test;

public class ServerLocationJUnitTest {

  @Test
  public void testEquals() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    assertEquals(serverLocation1, serverLocation2);
  }

  @Test
  public void testEquals_differentMemberId() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId2");
    assertNotEquals(serverLocation1, serverLocation2);
  }

  @Test
  public void testEquals_differentHostname() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.2", 1234, "dummyMemberId1");
    assertNotEquals(serverLocation1, serverLocation2);
  }

  @Test
  public void testEquals_differentPort() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.1", 4321, "dummyMemberId1");
    assertNotEquals(serverLocation1, serverLocation2);
  }

  @Test
  public void testConstructor_twoArgs() {
    ServerLocation serverLocation = new ServerLocation("host", 1234);
    assertEquals(serverLocation.getHostName(), "host");
    assertEquals(serverLocation.getPort(), 1234);
    assertEquals(serverLocation.getMemberId(), "");
  }

  @Test
  public void testConstructor_threeArgs() {
    ServerLocation serverLocation = new ServerLocation("host", 1234, "id");
    assertEquals(serverLocation.getHostName(), "host");
    assertEquals(serverLocation.getPort(), 1234);
    assertEquals(serverLocation.getMemberId(), "id");
  }

  @Test
  public void testHashCode_equals() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    assertEquals(serverLocation1.hashCode(), serverLocation2.hashCode());
  }

  @Test
  public void testHashCode_differentHostname() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.2", 1234, "dummyMemberId1");
    assertEquals(serverLocation1.hashCode(), serverLocation2.hashCode());
  }

  @Test
  public void testHashCode_differentPort() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.1", 4321, "dummyMemberId1");
    assertNotEquals(serverLocation1.hashCode(), serverLocation2.hashCode());
  }

  @Test
  public void testHashCode_differentMemberId() {
    ServerLocation serverLocation1 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId1");
    ServerLocation serverLocation2 = new ServerLocation("10.0.0.1", 1234, "dummyMemberId2");
    assertNotEquals(serverLocation1.hashCode(), serverLocation2.hashCode());
  }
}
