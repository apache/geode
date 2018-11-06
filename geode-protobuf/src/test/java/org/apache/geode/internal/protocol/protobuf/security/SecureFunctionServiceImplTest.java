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
package org.apache.geode.internal.protocol.protobuf.security;

import static org.apache.geode.security.ResourcePermission.ALL;
import static org.apache.geode.security.ResourcePermission.Operation.WRITE;
import static org.apache.geode.security.ResourcePermission.Resource.CLUSTER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class SecureFunctionServiceImplTest {
  public static final String REGION = "TestRegion";
  public static final String FUNCTION_ID = "id";
  private SecureFunctionServiceImpl functionService;
  private InternalCache cache;
  private Security security;
  private Region region;
  private Function function;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    region = mock(Region.class);
    when(cache.getRegion(REGION)).thenReturn(region);
    security = mock(Security.class);
    doThrow(NotAuthorizedException.class).when(security).authorize(any());
    doThrow(NotAuthorizedException.class).when(security).authorize(any(), any(), any(), any());
    functionService = new SecureFunctionServiceImpl(cache, security);
    function = mock(Function.class);
    when(function.getId()).thenReturn("id");
    FunctionService.registerFunction(function);
  }

  @After
  public void tearDown() {
    FunctionService.unregisterFunction(FUNCTION_ID);
  }

  @Test
  public void executeFunctionOnRegionWithoutAuthorization() throws Exception {
    when(function.getRequiredPermissions(REGION, null))
        .thenReturn(Collections.singleton(new ResourcePermission(CLUSTER, WRITE, REGION, ALL)));
    assertThatThrownBy(
        () -> functionService.executeFunctionOnRegion(FUNCTION_ID, REGION, null, null))
            .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void executeFunctionOnMemberWithoutAuthorization() throws Exception {
    when(function.getRequiredPermissions(null, null))
        .thenReturn(Collections.singleton(new ResourcePermission(CLUSTER, WRITE, REGION, ALL)));
    assertThatThrownBy(
        () -> functionService.executeFunctionOnMember(FUNCTION_ID, null, Arrays.asList("member")))
            .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void executeFunctionOnGroupsWithoutAuthorization() throws Exception {
    when(function.getRequiredPermissions(null, null))
        .thenReturn(Collections.singleton(new ResourcePermission(CLUSTER, WRITE, REGION, ALL)));
    assertThatThrownBy(
        () -> functionService.executeFunctionOnGroups(FUNCTION_ID, null, Arrays.asList("group")))
            .isInstanceOf(NotAuthorizedException.class);
  }

  private void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, String region, String key) {
    doNothing().when(security).authorize(resource, operation, region, key);
    doNothing().when(security).authorize(new ResourcePermission(resource, operation, region, key));
  }

}
