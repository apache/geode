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
package org.apache.geode.security;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({UnitTest.class, SecurityTest.class})
public class SimpleSecurityManagerTest {
  private SimpleTestSecurityManager manager;
  private Properties credentials;

  @Before
  public void before() {
    manager = new SimpleTestSecurityManager();
    credentials = new Properties();
  }

  @Test
  public void testAuthenticateSuccess() {
    credentials.put("security-username", "user");
    credentials.put("security-password", "user");
    assertEquals("user", manager.authenticate(credentials));
  }

  @Test
  public void testAuthenticateFail() {
    credentials.put("security-username", "user1");
    credentials.put("security-password", "user2");
    assertThatThrownBy(() -> manager.authenticate(credentials))
        .isInstanceOf(AuthenticationFailedException.class);
  }

  @Test
  public void testAuthenticateFailNull() {
    assertThatThrownBy(() -> manager.authenticate(credentials))
        .isInstanceOf(AuthenticationFailedException.class);
  }

  @Test
  public void testAuthorization() {
    ResourcePermission permission = new ResourcePermission("CLUSTER", "READ");
    assertTrue(manager.authorize("clusterRead", permission));
    assertTrue(manager.authorize("cluster", permission));
    assertFalse(manager.authorize("data", permission));

    permission = new ResourcePermission("DATA", "WRITE", "regionA", "key1");
    assertTrue(manager.authorize("data", permission));
    assertTrue(manager.authorize("dataWrite", permission));
    assertTrue(manager.authorize("dataWriteRegionA", permission));
    assertTrue(manager.authorize("dataWriteRegionAKey1", permission));
    assertFalse(manager.authorize("dataRead", permission));
  }

  @Test
  public void testMultipleRoleAuthorization() {
    ResourcePermission permission = new ResourcePermission("CLUSTER", "READ");
    assertTrue(manager.authorize("clusterRead,clusterWrite", permission));
    assertTrue(manager.authorize("cluster,data", permission));
    assertFalse(manager.authorize("clusterWrite,data", permission));

    permission = new ResourcePermission("DATA", "WRITE", "regionA", "key1");
    assertTrue(manager.authorize("data,cluster", permission));
    assertTrue(manager.authorize("dataWrite,clusterWrite", permission));
  }

}
