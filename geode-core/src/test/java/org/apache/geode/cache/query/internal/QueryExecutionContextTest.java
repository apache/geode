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
package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.internal.cache.InternalCache;


public class QueryExecutionContextTest {
  private QueryExecutionContext context;

  @Before
  public void setUp() {
    QueryConfigurationService mockService = mock(QueryConfigurationService.class);
    when(mockService.getMethodAuthorizer()).thenReturn(mock(MethodInvocationAuthorizer.class));
    InternalCache mockCache = mock(InternalCache.class);
    when(mockCache.getService(QueryConfigurationService.class)).thenReturn(mockService);

    context = new QueryExecutionContext(null, mockCache);
  }

  @Test
  public void testNullReturnedFromCacheGetWhenNoValueWasPut() {
    Object key = new Object();
    assertThat(context.cacheGet(key)).isNull();
  }

  @Test
  public void testPutValueReturnedFromCacheGet() {
    Object key = new Object();
    Object value = new Object();

    context.cachePut(key, value);
    assertThat(context.cacheGet(key)).isEqualTo(value);
  }

  @Test
  public void testDefaultReturnedFromCacheGetWhenNoValueWasPut() {
    Object key = new Object();
    Object value = new Object();

    assertThat(context.cacheGet(key, value)).isEqualTo(value);
  }

  @Test
  public void testExecCachesCanBePushedAndValuesRetrievedAtTheCorrectLevel() {
    Object key = new Object();
    Object value = new Object();

    context.pushExecCache(1);
    context.cachePut(key, value);
    context.pushExecCache(2);
    assertThat(context.cacheGet(key)).isNull();

    context.popExecCache();
    assertThat(context.cacheGet(key)).isEqualTo(value);
  }
}
