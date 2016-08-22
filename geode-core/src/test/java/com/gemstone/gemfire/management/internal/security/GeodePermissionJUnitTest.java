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

import org.apache.geode.security.GeodePermission;
import org.apache.geode.security.GeodePermission.Operation;
import org.apache.geode.security.GeodePermission.Resource;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category({ UnitTest.class, SecurityTest.class })
public class GeodePermissionJUnitTest {

  private GeodePermission context;

  @Test
  public void testEmptyConstructor(){
    context = new GeodePermission();
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(GeodePermission.ALL_REGIONS, context.getRegionName());
  }

  @Test
  public void testIsPermission(){
    context = new GeodePermission();
    assertTrue(context instanceof WildcardPermission);
  }

  @Test
  public void testConstructor(){
    context = new GeodePermission();
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(GeodePermission.ALL_REGIONS, context.getRegionName());

    context = new GeodePermission();
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(GeodePermission.ALL_REGIONS, context.getRegionName());

    context = new GeodePermission("DATA", null, null);
    assertEquals(Resource.DATA, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(GeodePermission.ALL_REGIONS, context.getRegionName());

    context = new GeodePermission("CLUSTER", null, null);
    assertEquals(Resource.CLUSTER, context.getResource());
    assertEquals(Operation.NULL, context.getOperation());
    assertEquals(GeodePermission.ALL_REGIONS, context.getRegionName());

    context = new GeodePermission(null, "MANAGE", "REGIONA");
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(Operation.MANAGE, context.getOperation());
    assertEquals("REGIONA", context.getRegionName());

    context = new GeodePermission("DATA", "MANAGE", "REGIONA");
    assertEquals(Resource.DATA, context.getResource());
    assertEquals(Operation.MANAGE, context.getOperation());
    assertEquals("REGIONA", context.getRegionName());
  }

  @Test
  public void testToString(){
    context = new GeodePermission();
    assertEquals("NULL:NULL", context.toString());

    context = new GeodePermission("DATA", "MANAGE");
    assertEquals("DATA:MANAGE", context.toString());

    context = new GeodePermission("DATA", "MANAGE", "REGIONA");
    assertEquals("DATA:MANAGE:REGIONA", context.toString());
  }
}
