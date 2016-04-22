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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.json.JSONException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.management.internal.security.JSONAuthorization.User;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests JSONAuthorization with JSON loaded from files.
 */
@Category(IntegrationTest.class)
public class JSONAuthorizationDetailsIntegrationTest {

  @Test
  public void testSimpleUserAndRole() throws Exception {
    String json = readFile(TestUtil.getResourcePath(getClass(), "testSimpleUserAndRole.json"));
    new JSONAuthorization(json); // static side effect
    Map<String, User> acl = JSONAuthorization.getAcl();
    assertNotNull(acl);
    assertEquals(1, acl.size());
    User user = acl.get("tushark");
    assertNotNull(user);
    assertNotNull(user.roles);
    assertEquals(1, user.roles.length);
    assertEquals("jmxReader", user.roles[0].name);
    assertEquals(1, user.roles[0].permissions.length);
    assertEquals("QUERY", user.roles[0].permissions[0]);
  }

  @Test
  public void testUserAndRoleRegionServerGroup() throws Exception {
    String json = readFile(TestUtil.getResourcePath(getClass(), "testUserAndRoleRegionServerGroup.json"));
    new JSONAuthorization(json); // static side effect
    Map<String, User> acl = JSONAuthorization.getAcl();
    
    assertNotNull(acl);
    assertEquals(1, acl.size());
    User user = acl.get("tushark");
    assertNotNull(user);
    assertNotNull(user.roles);
    assertEquals(1, user.roles.length);
    assertEquals("jmxReader", user.roles[0].name);
    assertEquals(1, user.roles[0].permissions.length);
    assertEquals("QUERY", user.roles[0].permissions[0]);

    assertEquals("secureRegion", user.roles[0].regionName);
    assertEquals("SG2", user.roles[0].serverGroup);
  }

  @Test
  public void testUserMultipleRole() throws Exception {
    String json = readFile(TestUtil.getResourcePath(getClass(), "testUserMultipleRole.json"));
    new JSONAuthorization(json); // static side effect
    Map<String, User> acl = JSONAuthorization.getAcl();
    
    assertNotNull(acl);
    assertEquals(1, acl.size());
    User user = acl.get("tushark");
    assertNotNull(user);
    assertNotNull(user.roles);
    assertEquals(2, user.roles.length);

    JSONAuthorization.Role role = user.roles[0];
    assertEquals("jmxReader", role.name);

    assertEquals(1, role.permissions.length);
    assertEquals("QUERY", role.permissions[0]);

    role = user.roles[1];
    assertNotEquals("jmxReader", role.name);

    assertEquals(7, role.permissions.length);
    assertEquals("sysMonitors", role.name);
    assertTrue(contains(role.permissions, "CMD_EXORT_LOGS"));
    assertTrue(contains(role.permissions, "CMD_STACK_TRACES"));
    assertTrue(contains(role.permissions, "CMD_GC"));
    assertTrue(contains(role.permissions, "CMD_NETSTAT"));
    assertTrue(contains(role.permissions, "CMD_SHOW_DEADLOCKS"));
    assertTrue(contains(role.permissions, "CMD_SHOW_LOG"));
    assertTrue(contains(role.permissions, "SHOW_METRICS"));
  }

  @Test
  public void testInheritRole() throws Exception {
    String json = readFile(TestUtil.getResourcePath(getClass(), "testInheritRole.json"));
    new JSONAuthorization(json); // static side effect
    Map<String, User> acl = JSONAuthorization.getAcl();
    
    assertNotNull(acl);
    assertEquals(3, acl.size());
    User user = acl.get("tushark");
    assertNotNull(user);
    assertNotNull(user.roles);
    assertEquals(1, user.roles.length);
    assertEquals("jmxReader", user.roles[0].name);
    assertEquals(1, user.roles[0].permissions.length);
    assertEquals("QUERY", user.roles[0].permissions[0]);

    User admin1 = acl.get("admin1");
    assertNotNull(admin1);
    assertNotNull(admin1.roles);
    assertEquals(1, admin1.roles.length);
    assertEquals("adminSG1", admin1.roles[0].name);
    assertEquals("SG1", admin1.roles[0].serverGroup);
    assertEquals(1, admin1.roles[0].permissions.length);
    assertEquals("CMD_SHUTDOWN", admin1.roles[0].permissions[0]);

    User admin2 = acl.get("admin2");
    assertNotNull(admin2);
    assertNotNull(admin2.roles);
    assertEquals(1, admin2.roles.length);
    assertEquals("adminSG2", admin2.roles[0].name);
    assertEquals("SG2", admin2.roles[0].serverGroup);
    assertEquals(2, admin2.roles[0].permissions.length);
    assertTrue(contains(admin2.roles[0].permissions, "CHANGE_LOG_LEVEL"));
    assertTrue(contains(admin2.roles[0].permissions, "CMD_SHUTDOWN"));
  }

  private String readFile(String name) throws IOException, JSONException {
    File file = new File(name);
    FileReader reader = new FileReader(file);
    char[] buffer = new char[(int) file.length()];
    reader.read(buffer);
    String json = new String(buffer);
    reader.close();
    return json;
  }

  private boolean contains(String[] permissions, String string) {
    for (String str : permissions) {
      if (str.equals(string)) {
        return true;
      }
    }
    return false;
  }
}
