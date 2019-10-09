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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.NameNotFoundException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.NotAuthorizedException;

public class MethodDispatchTest {
  private List emptyList;
  private TestBean testBean;
  private QueryExecutionContext queryExecutionContext;
  private MethodInvocationAuthorizer methodInvocationAuthorizer;

  @Before
  public void setUp() {
    testBean = new TestBean();
    emptyList = Collections.emptyList();
    methodInvocationAuthorizer = spy(MethodInvocationAuthorizer.class);
    queryExecutionContext = new QueryExecutionContext(null, mock(InternalCache.class));
  }

  @Test
  public void invokeShouldReturnCorrectlyWhenMethodIsAuthorized()
      throws NameResolutionException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch = new MethodDispatch(methodInvocationAuthorizer, TestBean.class,
        "publicMethod", Collections.emptyList());

    Object result = methodDispatch.invoke(testBean, emptyList, queryExecutionContext);
    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo("publicMethod");
  }

  @Test
  public void invokeShouldThrowExceptionWhenMethodIsNotAuthorized() throws NameResolutionException {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch = new MethodDispatch(methodInvocationAuthorizer, TestBean.class,
        "publicMethod", Collections.emptyList());

    assertThatThrownBy(() -> methodDispatch.invoke(testBean, emptyList, queryExecutionContext))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + "publicMethod");
  }

  @Test
  public void invokeShouldUseAlreadyAuthorizedCachedResultWhenMethodIsAuthorized()
      throws NameResolutionException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch = new MethodDispatch(methodInvocationAuthorizer, TestBean.class,
        "publicMethod", Collections.emptyList());

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        Object result = methodDispatch.invoke(testBean, emptyList, queryExecutionContext);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("publicMethod");
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(1)).authorize(any(), any());

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        QueryExecutionContext mockContext = mock(QueryExecutionContext.class);
        Object result = methodDispatch.invoke(testBean, emptyList, mockContext);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("publicMethod");
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(11)).authorize(any(), any());
  }

  @Test
  public void invokeShouldUseAlreadyAuthorizedCachedResultWhenMethodIsNotAuthorized()
      throws NameResolutionException {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch = new MethodDispatch(methodInvocationAuthorizer, TestBean.class,
        "publicMethod", Collections.emptyList());

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> assertThatThrownBy(
        () -> methodDispatch.invoke(testBean, emptyList, queryExecutionContext))
            .isInstanceOf(NotAuthorizedException.class)
            .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + "publicMethod"));
    verify(methodInvocationAuthorizer, times(1)).authorize(any(), any());

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> assertThatThrownBy(
        () -> methodDispatch.invoke(testBean, emptyList, mock(QueryExecutionContext.class)))
            .isInstanceOf(NotAuthorizedException.class)
            .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + "publicMethod"));
    verify(methodInvocationAuthorizer, times(11)).authorize(any(), any());
  }

  @SuppressWarnings("unused")
  private static class TestBean {

    public String publicMethod() {
      return "publicMethod";
    }

    private String privateMethod() {
      return "privateMethod";
    }
  }
}
