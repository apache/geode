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

import org.junit.Test;

import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.internal.cache.InternalCache;

public class ExecutionContextTest {

  @Test
  public void constructorShouldUseConfiguredMethodAuthorizer() {
    InternalCache mockCache = mock(InternalCache.class);
    QueryConfigurationService mockService = mock(QueryConfigurationService.class);
    when(mockCache.getService(QueryConfigurationService.class)).thenReturn(mockService);

    MethodInvocationAuthorizer noOpAuthorizer = QueryConfigurationServiceImpl.getNoOpAuthorizer();
    when(mockService.getMethodAuthorizer()).thenReturn(noOpAuthorizer);
    ExecutionContext executionContextNoOpAuthorizer = new ExecutionContext(null, mockCache);
    assertThat(executionContextNoOpAuthorizer.getMethodInvocationAuthorizer())
        .isSameAs(noOpAuthorizer);

    MethodInvocationAuthorizer restrictedAuthorizer = new RestrictedMethodAuthorizer(mockCache);
    when(mockService.getMethodAuthorizer()).thenReturn(restrictedAuthorizer);
    ExecutionContext executionContextRestrictedAuthorizer = new ExecutionContext(null, mockCache);
    assertThat(executionContextRestrictedAuthorizer.getMethodInvocationAuthorizer())
        .isSameAs(restrictedAuthorizer);
  }
}
