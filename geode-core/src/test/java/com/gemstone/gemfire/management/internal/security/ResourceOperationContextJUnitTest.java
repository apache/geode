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

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.operations.OperationContext.Resource;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ResourceOperationContextJUnitTest {

  private ResourceOperationContext context;

  @Test
  public void testEmptyConstructor(){
    context = new ResourceOperationContext();
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(OperationCode.NULL, context.getOperationCode());
    assertEquals(null, context.getRegionName());
  }

  @Test
  public void testIsPermission(){
    context = new ResourceOperationContext();
    assertTrue(context instanceof WildcardPermission);
  }

  @Test
  public void testConstructor(){
    context = new ResourceOperationContext(null, null, null);
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(OperationCode.NULL, context.getOperationCode());
    assertEquals(null, context.getRegionName());

    context = new ResourceOperationContext(null, null);
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(OperationCode.NULL, context.getOperationCode());
    assertEquals(null, context.getRegionName());

    context = new ResourceOperationContext("DATA", null, null);
    assertEquals(Resource.DATA, context.getResource());
    assertEquals(OperationCode.NULL, context.getOperationCode());
    assertEquals("NULL", context.getRegionName());

    context = new ResourceOperationContext("CLUSTER", null, null);
    assertEquals(Resource.CLUSTER, context.getResource());
    assertEquals(OperationCode.NULL, context.getOperationCode());
    assertEquals(null, context.getRegionName());

    context = new ResourceOperationContext(null, "MANAGE", "REGIONA");
    assertEquals(Resource.NULL, context.getResource());
    assertEquals(OperationCode.MANAGE, context.getOperationCode());
    assertEquals("REGIONA", context.getRegionName());

    context = new ResourceOperationContext("DATA", "MANAGE", "REGIONA");
    assertEquals(Resource.DATA, context.getResource());
    assertEquals(OperationCode.MANAGE, context.getOperationCode());
    assertEquals("REGIONA", context.getRegionName());
  }

  @Test
  public void testToString(){
    context = new ResourceOperationContext();
    assertEquals("NULL:NULL", context.toString());

    context = new ResourceOperationContext("DATA", "MANAGE");
    assertEquals("DATA:MANAGE:NULL", context.toString());

    context = new ResourceOperationContext("DATA", "MANAGE", "REGIONA");
    assertEquals("DATA:MANAGE:REGIONA", context.toString());
  }
}
