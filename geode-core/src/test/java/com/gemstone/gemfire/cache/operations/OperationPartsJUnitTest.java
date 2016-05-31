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
package com.gemstone.gemfire.cache.operations;

import com.gemstone.gemfire.cache.operations.internal.GetOperationContextImpl;
import com.gemstone.gemfire.internal.cache.operations.ContainsKeyOperationContext;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({ UnitTest.class, SecurityTest.class })
public class OperationPartsJUnitTest {

  private static final WildcardPermission allDataPermissions =
      new WildcardPermission(OperationContext.Resource.DATA.toString(), true);

  private Permission makePermission(OperationContext.OperationCode opCode) {
    return new WildcardPermission(String.format("%s:%s", OperationContext.Resource.DATA, opCode.toString()), true);
  }

  @Test
  public void closeCQ() {
    CloseCQOperationContext context = new CloseCQOperationContext("name", "query", new HashSet<>());
    assertTrue(makePermission(OperationContext.OperationCode.CLOSE_CQ).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void containsKey() {
    ContainsKeyOperationContext context = new ContainsKeyOperationContext(null);
    assertTrue(makePermission(OperationContext.OperationCode.CONTAINS_KEY).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void destroy() {
    DestroyOperationContext context = new DestroyOperationContext(null);
    assertTrue(makePermission(OperationContext.OperationCode.DESTROY).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void executeCQ() {
    ExecuteCQOperationContext context = new ExecuteCQOperationContext("name", "query", new HashSet<>(), false);
    assertTrue(makePermission(OperationContext.OperationCode.EXECUTE_CQ).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void executeFunction() {
    ExecuteFunctionOperationContext context = new ExecuteFunctionOperationContext("name", "region", new HashSet<>(), null, true, false);
    assertTrue(makePermission(OperationContext.OperationCode.EXECUTE_FUNCTION).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void getDurableCQs() {
    GetDurableCQsOperationContext context = new GetDurableCQsOperationContext();
    assertTrue(makePermission(OperationContext.OperationCode.GET_DURABLE_CQS).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void get() {
    GetOperationContext context = new GetOperationContextImpl(null, false);
    assertTrue(makePermission(OperationContext.OperationCode.GET).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void registerInterest() {
    RegisterInterestOperationContext context = new RegisterInterestOperationContext(null, null, null);
    assertTrue(makePermission(OperationContext.OperationCode.REGISTER_INTEREST).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void unregisterInterest() {
    UnregisterInterestOperationContext context = new UnregisterInterestOperationContext(null, null);
    assertTrue(makePermission(OperationContext.OperationCode.UNREGISTER_INTEREST).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void invalidate() {
    InvalidateOperationContext context = new InvalidateOperationContext(null);
    assertTrue(makePermission(OperationContext.OperationCode.INVALIDATE).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void keySet() {
    KeySetOperationContext context = new KeySetOperationContext(false);
    assertTrue(makePermission(OperationContext.OperationCode.KEY_SET).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void putAll() {
    PutAllOperationContext context = new PutAllOperationContext(new HashMap<>());
    assertTrue(makePermission(OperationContext.OperationCode.PUTALL).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void put() {
    PutOperationContext context = new PutOperationContext(null, null, true);
    assertTrue(makePermission(OperationContext.OperationCode.PUT).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void query() {
    QueryOperationContext context = new QueryOperationContext("query", null, false);
    assertTrue(makePermission(OperationContext.OperationCode.QUERY).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void regionClear() {
    RegionClearOperationContext context = new RegionClearOperationContext(false);
    assertTrue(makePermission(OperationContext.OperationCode.REGION_CLEAR).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void regionCreate() {
    RegionCreateOperationContext context = new RegionCreateOperationContext(false);
    assertTrue(makePermission(OperationContext.OperationCode.REGION_CREATE).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void regionDestroy() {
    RegionDestroyOperationContext context = new RegionDestroyOperationContext(false);
    assertTrue(makePermission(OperationContext.OperationCode.REGION_DESTROY).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void removeAll() {
    RemoveAllOperationContext context = new RemoveAllOperationContext(null);
    assertTrue(makePermission(OperationContext.OperationCode.REMOVEALL).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }

  @Test
  public void stopCQ() {
    StopCQOperationContext context = new StopCQOperationContext(null, null, null);
    assertTrue(makePermission(OperationContext.OperationCode.STOP_CQ).implies(context));
    assertTrue(allDataPermissions.implies(context));
    assertFalse(context.isPostOperation());
  }
}
