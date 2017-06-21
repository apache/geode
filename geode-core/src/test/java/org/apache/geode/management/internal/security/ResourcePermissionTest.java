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
package org.apache.geode.management.internal.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.UnitTest;

@Category({UnitTest.class, SecurityTest.class})
public class ResourcePermissionTest {
  private ResourcePermission context;

  @Test
  public void testEmptyConstructor() {
    context = new ResourcePermission();
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(ResourcePermission.ALL, context.getTarget());
  }

  @Test
  public void testIsPermission() {
    context = new ResourcePermission();
    assertTrue(context instanceof WildcardPermission);
  }

  @Test
  public void testConstructor() {
    context = new ResourcePermission();
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(ResourcePermission.ALL, context.getTarget());

    context = new ResourcePermission();
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(ResourcePermission.ALL, context.getTarget());

    context = new ResourcePermission(Resource.DATA, null);
    assertEquals(Resource.DATA, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(ResourcePermission.ALL, context.getTarget());

    context = new ResourcePermission(Resource.CLUSTER, null);
    assertEquals(Resource.CLUSTER, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(ResourcePermission.ALL, context.getTarget());

    context = new ResourcePermission(null, Operation.MANAGE, "REGIONA");
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.MANAGE, context.getOperation());
    assertEquals("REGIONA", context.getTarget());

    context = new ResourcePermission(Resource.DATA, Operation.MANAGE, "REGIONA");
    assertEquals(Resource.DATA, context.getResource());
    assertEquals(Operation.MANAGE, context.getOperation());
    assertEquals("REGIONA", context.getTarget());

    context = new ResourcePermission(Resource.CLUSTER, Operation.MANAGE);
    assertEquals(Resource.CLUSTER, context.getResource());
    assertEquals(Operation.MANAGE, context.getOperation());
    assertEquals(ResourcePermission.ALL, context.getTarget());

    // make sure "ALL" in the resource "DATA" means regionName won't be converted to *
    context = new ResourcePermission(Resource.DATA, Operation.READ, "ALL");
    assertEquals(Resource.DATA, context.getResource());
    assertEquals(Operation.READ, context.getOperation());
    assertEquals("ALL", context.getTarget());

    context = new ResourcePermission(Resource.CLUSTER, Operation.READ, Target.ALL);
    assertEquals(context.getTarget(), ResourcePermission.ALL);
  }

  @Test
  public void testToString() {
    context = new ResourcePermission();
    assertEquals("NULL:NULL", context.toString());

    context = new ResourcePermission(Resource.DATA, Operation.MANAGE);
    assertEquals("DATA:MANAGE", context.toString());

    context = new ResourcePermission(Resource.DATA, Operation.MANAGE, "REGIONA");
    assertEquals("DATA:MANAGE:REGIONA", context.toString());

    context = new ResourcePermission(Resource.DATA, Operation.MANAGE);
    assertEquals("DATA:MANAGE", context.toString());
  }

  @Test
  public void testImplies() {
    WildcardPermission role = new WildcardPermission("*:read");
    role.implies(new ResourcePermission(Resource.DATA, Operation.READ));
    role.implies(new ResourcePermission(Resource.CLUSTER, Operation.READ));

    role = new WildcardPermission("*:read:*");
    role.implies(new ResourcePermission(Resource.DATA, Operation.READ, "testRegion"));
    role.implies(new ResourcePermission(Resource.CLUSTER, Operation.READ, "anotherRegion", "key1"));

    role = new WildcardPermission("data:*:testRegion");
    role.implies(new ResourcePermission(Resource.DATA, Operation.READ, "testRegion"));
    role.implies(new ResourcePermission(Resource.DATA, Operation.WRITE, "testRegion"));
  }
}
