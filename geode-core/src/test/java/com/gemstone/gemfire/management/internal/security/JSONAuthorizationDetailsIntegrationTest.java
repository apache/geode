/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.management.internal.security.JSONAuthorization.User;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests JSONAuthorization with JSON loaded from files.
 */
@Category(IntegrationTest.class)
public class JSONAuthorizationDetailsIntegrationTest {

  @Test
  public void testSimpleUserAndRole() throws Exception {
    new JSONAuthorization("testSimpleUserAndRole.json");
    Map<String, User> acl = JSONAuthorization.getAcl();
    assertNotNull(acl);
    assertEquals(1, acl.size());
    User user = acl.get("tushark");
    assertNotNull(user);
    assertEquals(1, user.permissions.size());
    JSONAuthorization.Permission p = user.permissions.iterator().next();
    assertEquals("QUERY:EXECUTE", p.toString());
  }

  @Test
  public void testUserAndRoleRegionServerGroup() throws Exception {
    new JSONAuthorization("testUserAndRoleRegionServerGroup.json");
    Map<String, User> acl = JSONAuthorization.getAcl();

    assertNotNull(acl);
    assertEquals(1, acl.size());
    User user = acl.get("tushark");
    assertNotNull(user);
    assertEquals(1, user.permissions.size());
    JSONAuthorization.Permission p = user.permissions.iterator().next();
    assertEquals("secureRegion", p.getRegion());
  }

  @Test
  public void testUserMultipleRole() throws Exception {
    new JSONAuthorization("testUserMultipleRole.json");
    Map<String, User> acl = JSONAuthorization.getAcl();

    assertNotNull(acl);
    assertEquals(1, acl.size());
    User user = acl.get("tushark");
    assertNotNull(user);
    assertEquals(3, user.permissions.size());
  }
}
