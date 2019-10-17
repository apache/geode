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
import static org.mockito.Mockito.when;

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
  private static final String PUBLIC_METHOD_NAME = "publicMethod";
  private static final String PUBLIC_METHOD_RETURN_VALUE = "public";
  private static final String ANOTHER_PUBLIC_METHOD_NAME = "anotherPublicMethod";
  private static final String ANOTHER_PUBLIC_METHOD_RETURN_VALUE = "anotherPublic";
  private static final String EXTENDED_PUBLIC_METHOD_NAME = "publicMethod";
  private static final String EXTENDED_PUBLIC_METHOD_RETURN_VALUE = "extendedPublic";

  private List emptyList;
  private TestBean testBean;
  private QueryExecutionContext queryExecutionContext;
  private MethodInvocationAuthorizer methodInvocationAuthorizer;

  @Before
  public void setUp() {
    testBean = new TestBean();
    emptyList = Collections.emptyList();
    methodInvocationAuthorizer = spy(MethodInvocationAuthorizer.class);

    InternalCache mockCache = mock(InternalCache.class);
    InternalQueryService mockQueryService = mock(InternalQueryService.class);
    when(mockCache.getQueryService()).thenReturn(mockQueryService);
    when(mockQueryService.getMethodInvocationAuthorizer()).thenReturn(methodInvocationAuthorizer);
    queryExecutionContext = new QueryExecutionContext(null, mockCache);
  }

  @Test
  public void invokeShouldReturnCorrectlyWhenMethodIsAuthorized()
      throws NameResolutionException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch =
        new MethodDispatch(TestBean.class, PUBLIC_METHOD_NAME, Collections.emptyList());

    Object result = methodDispatch.invoke(testBean, emptyList, queryExecutionContext);
    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo(PUBLIC_METHOD_RETURN_VALUE);
  }

  @Test
  public void invokeShouldThrowExceptionWhenMethodIsNotAuthorized() throws NameResolutionException {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch =
        new MethodDispatch(TestBean.class, PUBLIC_METHOD_NAME, Collections.emptyList());

    assertThatThrownBy(() -> methodDispatch.invoke(testBean, emptyList, queryExecutionContext))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + PUBLIC_METHOD_NAME);
  }

  @Test
  public void invokeShouldCacheResultForAllowedMethod() throws Exception {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch =
        new MethodDispatch(Integer.class, "toString", Collections.emptyList());

    methodDispatch.invoke(new Integer("0"), emptyList, queryExecutionContext);
    assertThat(queryExecutionContext.cacheGet("java.lang.Integer.toString")).isEqualTo(true);
  }

  @Test
  public void invokeShouldCacheResultForForbiddenMethod() throws Exception {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch methodDispatch =
        new MethodDispatch(Integer.class, "toString", Collections.emptyList());

    assertThatThrownBy(
        () -> methodDispatch.invoke(new Integer("0"), emptyList, queryExecutionContext))
            .isInstanceOf(NotAuthorizedException.class)
            .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + "toString");
    assertThat(queryExecutionContext.cacheGet("java.lang.Integer.toString")).isEqualTo(false);
  }

  @Test
  public void invokeShouldUseCachedAuthorizerResultWhenMethodIsAuthorizedAndQueryContextIsTheSame()
      throws NameResolutionException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch publicMethodDispatch =
        new MethodDispatch(TestBean.class, PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch anotherPublicMethodDispatch =
        new MethodDispatch(TestBean.class, ANOTHER_PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch extendedPublicMethodDispatch =
        new MethodDispatch(TestBeanExtension.class, EXTENDED_PUBLIC_METHOD_NAME,
            Collections.emptyList());

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        assertThat(publicMethodDispatch.invoke(testBean, emptyList, queryExecutionContext))
            .isInstanceOf(String.class).isEqualTo(PUBLIC_METHOD_RETURN_VALUE);
        assertThat(anotherPublicMethodDispatch.invoke(testBean, emptyList, queryExecutionContext))
            .isInstanceOf(String.class).isEqualTo(ANOTHER_PUBLIC_METHOD_RETURN_VALUE);
        assertThat(extendedPublicMethodDispatch
            .invoke(new TestBeanExtension(), emptyList, queryExecutionContext))
                .isInstanceOf(String.class).isEqualTo(EXTENDED_PUBLIC_METHOD_RETURN_VALUE);
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(3)).authorize(any(), any());
  }

  @Test
  public void invokeShouldNotUseCachedAuthorizerResultWhenMethodIsAuthorizedAndQueryContextIsNotTheSame()
      throws NameResolutionException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch publicMethodDispatch =
        new MethodDispatch(TestBean.class, PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch anotherPublicMethodDispatch =
        new MethodDispatch(TestBean.class, ANOTHER_PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch extendedPublicMethodDispatch =
        new MethodDispatch(TestBeanExtension.class, EXTENDED_PUBLIC_METHOD_NAME,
            Collections.emptyList());

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        QueryExecutionContext mockContext = mock(QueryExecutionContext.class);
        when(mockContext.getMethodInvocationAuthorizer()).thenReturn(methodInvocationAuthorizer);

        assertThat(publicMethodDispatch.invoke(testBean, emptyList, mockContext))
            .isInstanceOf(String.class).isEqualTo(PUBLIC_METHOD_RETURN_VALUE);
        assertThat(anotherPublicMethodDispatch.invoke(testBean, emptyList, mockContext))
            .isInstanceOf(String.class).isEqualTo(ANOTHER_PUBLIC_METHOD_RETURN_VALUE);
        assertThat(
            extendedPublicMethodDispatch.invoke(new TestBeanExtension(), emptyList, mockContext))
                .isInstanceOf(String.class).isEqualTo(EXTENDED_PUBLIC_METHOD_RETURN_VALUE);
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(30)).authorize(any(), any());
  }

  @Test
  public void invokeShouldUseCachedAuthorizerResultWhenMethodIsNotAuthorizedAndContextIsTheSame()
      throws NameResolutionException {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch publicMethodDispatch =
        new MethodDispatch(TestBean.class, PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch anotherPublicMethodDispatch =
        new MethodDispatch(TestBean.class, ANOTHER_PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch extendedPublicMethodDispatch =
        new MethodDispatch(TestBeanExtension.class, EXTENDED_PUBLIC_METHOD_NAME,
            Collections.emptyList());

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> {
      assertThatThrownBy(
          () -> publicMethodDispatch.invoke(testBean, emptyList, queryExecutionContext))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + PUBLIC_METHOD_NAME);
      assertThatThrownBy(
          () -> anotherPublicMethodDispatch.invoke(testBean, emptyList, queryExecutionContext))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessage(
                  RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + ANOTHER_PUBLIC_METHOD_NAME);
      assertThatThrownBy(() -> extendedPublicMethodDispatch
          .invoke(new TestBeanExtension(), emptyList, queryExecutionContext))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessage(
                  RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + EXTENDED_PUBLIC_METHOD_NAME);
    });
    verify(methodInvocationAuthorizer, times(3)).authorize(any(), any());
  }

  @Test
  public void invokeShouldNotUseCachedAuthorizerResultWhenMethodIsNotAuthorizedAndContextIsNotTheSame()
      throws NameResolutionException {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    MethodDispatch publicMethodDispatch =
        new MethodDispatch(TestBean.class, PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch anotherPublicMethodDispatch =
        new MethodDispatch(TestBean.class, ANOTHER_PUBLIC_METHOD_NAME, Collections.emptyList());
    MethodDispatch extendedPublicMethodDispatch =
        new MethodDispatch(TestBeanExtension.class, EXTENDED_PUBLIC_METHOD_NAME,
            Collections.emptyList());

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      QueryExecutionContext mockContext = mock(QueryExecutionContext.class);
      when(mockContext.getMethodInvocationAuthorizer()).thenReturn(methodInvocationAuthorizer);

      assertThatThrownBy(() -> publicMethodDispatch.invoke(testBean, emptyList, mockContext))
          .isInstanceOf(NotAuthorizedException.class)
          .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + PUBLIC_METHOD_NAME);
      assertThatThrownBy(() -> anotherPublicMethodDispatch.invoke(testBean, emptyList, mockContext))
          .isInstanceOf(NotAuthorizedException.class)
          .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + ANOTHER_PUBLIC_METHOD_NAME);
      assertThatThrownBy(() -> extendedPublicMethodDispatch
          .invoke(new TestBeanExtension(), emptyList, mockContext))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessage(
                  RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + EXTENDED_PUBLIC_METHOD_NAME);
    });
    verify(methodInvocationAuthorizer, times(30)).authorize(any(), any());
  }

  @SuppressWarnings("unused")
  private static class TestBean {
    public String publicMethod() {
      return PUBLIC_METHOD_RETURN_VALUE;
    }

    public String anotherPublicMethod() {
      return ANOTHER_PUBLIC_METHOD_RETURN_VALUE;
    }
  }

  private static class TestBeanExtension extends TestBean {
    @Override
    public String publicMethod() {
      return EXTENDED_PUBLIC_METHOD_RETURN_VALUE;
    }
  }
}
