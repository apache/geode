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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.stream.IntStream;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.query.NameNotFoundException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AttributeDescriptorTest {
  private static final String PUBLIC_NO_ACCESSORS = "publicAttributeWithoutAccessors";
  private static final String PUBLIC_ACCESSOR_BY_NAME = "publicAttributeWithPublicAccessor";
  private static final String PUBLIC_ACCESSOR_BY_GETTER = "publicAttributeWithPublicGetterMethod";
  private static final String PRIVATE_ACCESSOR_BY_NAME = "nonPublicAttributeWithPublicAccessor";
  private static final String PRIVATE_ACCESSOR_BY_GETTER =
      "nonPublicAttributeWithPublicGetterMethod";

  private TestBean testBean;
  private TypeRegistry typeRegistry;
  private QueryExecutionContext queryExecutionContext;
  private MethodInvocationAuthorizer methodInvocationAuthorizer;

  @Before
  public void setUp() {
    AttributeDescriptor._localCache.clear();
    testBean = new TestBean(PUBLIC_NO_ACCESSORS, PUBLIC_ACCESSOR_BY_NAME, PUBLIC_ACCESSOR_BY_GETTER,
        PRIVATE_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_GETTER);

    InternalCache mockCache = mock(InternalCache.class);
    typeRegistry = new TypeRegistry(mockCache, true);
    methodInvocationAuthorizer = spy(MethodInvocationAuthorizer.class);

    QueryConfigurationService mockService = mock(QueryConfigurationService.class);
    when(mockService.getMethodAuthorizer()).thenReturn(methodInvocationAuthorizer);

    when(mockCache.getService(QueryConfigurationService.class)).thenReturn(mockService);
    queryExecutionContext = spy(new QueryExecutionContext(null, mockCache));
  }

  @Test
  @Parameters({PUBLIC_NO_ACCESSORS, PUBLIC_ACCESSOR_BY_NAME, PUBLIC_ACCESSOR_BY_GETTER,
      PRIVATE_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_GETTER})
  public void validateReadTypeShouldReturnTrueWhenMemberCanBeFound(String attributeName) {
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);
    assertThat(attributeDescriptor.validateReadType(TestBean.class)).isTrue();
  }

  @Test
  public void validateReadTypeShouldReturnFalseWhenMemberCanNotBeFound() {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, "nonExistingAttribute");
    assertThat(attributeDescriptor.validateReadType(TestBean.class)).isFalse();
  }

  @Test
  @Parameters({PRIVATE_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_GETTER})
  public void getReadFieldShouldReturnNullForNonPublicAttributes(String attributeName) {
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);
    assertThat(attributeDescriptor.getReadField(TestBean.class)).isNull();
  }

  @Test
  @Parameters({PUBLIC_NO_ACCESSORS, PUBLIC_ACCESSOR_BY_NAME, PUBLIC_ACCESSOR_BY_GETTER})
  public void getReadFieldShouldReturnRequestedFieldForPublicAttributes(String attributeName)
      throws NoSuchFieldException {
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);
    assertThat(attributeDescriptor.getReadField(TestBean.class))
        .isEqualTo(TestBean.class.getField(attributeName));
  }

  @Test
  @Parameters({PUBLIC_ACCESSOR_BY_GETTER, PRIVATE_ACCESSOR_BY_GETTER})
  public void getReadMethodShouldReturnRequestedMethodForAttributesWithGetters(String attributeName)
      throws NoSuchMethodException {
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);
    String getterName = "get" + attributeName.substring(0, 1).toUpperCase()
        + attributeName.substring(1);
    assertThat(attributeDescriptor.getReadMethod(TestBean.class))
        .isEqualTo(TestBean.class.getMethod(getterName));
  }

  @Test
  @Parameters({PUBLIC_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_NAME})
  public void getReadMethodShouldReturnRequestedMethodForAttributesWithAccessors(
      String attributeName) throws NoSuchMethodException {
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);
    assertThat(attributeDescriptor.getReadMethod(TestBean.class, attributeName))
        .isEqualTo(TestBean.class.getMethod(attributeName));
  }

  @Test
  public void getReadMethodShouldReturnNullAndUpdateCachedClassToMethodsMapWhenMethodCanNotBeFound() {
    DefaultQuery.getPdxClasstoMethodsmap().clear();
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, "nonExistingAttribute");
    assertThat(attributeDescriptor.getReadMethod(TestBean.class)).isNull();
    assertThat(DefaultQuery.getPdxClasstoMethodsmap().isEmpty()).isFalse();
    assertThat(DefaultQuery.getPdxClasstoMethodsmap()
        .containsKey(TestBean.class.getCanonicalName())).isTrue();
    assertThat(DefaultQuery.getPdxClasstoMethodsmap().get(TestBean.class.getCanonicalName())
        .contains("nonExistingAttribute")).isTrue();
  }

  @Test
  @Parameters({PUBLIC_NO_ACCESSORS, PUBLIC_ACCESSOR_BY_NAME, PUBLIC_ACCESSOR_BY_GETTER})
  public void getReadMemberShouldReturnFieldWhenAttributeIsPublicAndUseInternalCache(
      String attributeName) throws NameNotFoundException {
    AttributeDescriptor attributeDescriptor =
        spy(new AttributeDescriptor(typeRegistry, attributeName));
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Field.class);
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Field.class);

    // Second time the field should be obtained from the local cache.
    verify(attributeDescriptor, times(1)).getReadField(TestBean.class);
  }

  @Test
  @Parameters({PRIVATE_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_GETTER})
  public void getReadMemberShouldReturnMethodWhenAttributeIsNotPublicAndUseInternalCache(
      String attributeName) throws NameNotFoundException {
    AttributeDescriptor attributeDescriptor =
        spy(new AttributeDescriptor(typeRegistry, attributeName));
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Method.class);
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Method.class);

    // Second time the field should be obtained from the local cache.
    verify(attributeDescriptor, times(1)).getReadMethod(TestBean.class);
  }

  @Test
  public void getReadMemberShouldThrowExceptionWhenMethodCanNotBeFound() {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, "nonExistingAttribute");
    assertThatThrownBy(() -> attributeDescriptor.getReadMember(TestBean.class))
        .isInstanceOf(NameNotFoundException.class)
        .hasMessageContaining(
            "No public attribute named ' nonExistingAttribute ' was found in class "
                + TestBean.class.getName());
  }

  @Test
  public void readReflectionShouldReturnUndefinedWhenTargetObjectIsAnInternalToken()
      throws NameNotFoundException, QueryInvocationTargetException {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, "nonExistingAttribute");
    assertThat(attributeDescriptor.readReflection(mock(Token.class), queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
  }

  @Test
  public void readReflectionShouldThrowExceptionWhenMemberCanNotBeFound() {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, "nonExistingAttribute");
    assertThatThrownBy(
        () -> attributeDescriptor.readReflection(TestBean.class, queryExecutionContext))
            .isInstanceOf(NameNotFoundException.class);
  }

  @Test
  public void readReflectionShouldReturnUndefinedWhenEntryDestroyedExceptionIsThrown()
      throws NameNotFoundException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor =
        spy(new AttributeDescriptor(typeRegistry, "throwEntryDestroyedExceptionMethod"));
    assertThat(attributeDescriptor.readReflection(testBean, queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
  }

  @Test
  @Parameters({PUBLIC_NO_ACCESSORS, PUBLIC_ACCESSOR_BY_NAME, PUBLIC_ACCESSOR_BY_GETTER})
  public void readReflectionForPublicAttributeShouldNotInvokeAuthorizer(String attributeName)
      throws NameResolutionException, QueryInvocationTargetException {
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);

    Object result = attributeDescriptor.readReflection(testBean, queryExecutionContext);
    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo(attributeName);
    verify(methodInvocationAuthorizer, never()).authorize(any(), any());
  }

  @Test
  @Parameters({PRIVATE_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_GETTER})
  public void readReflectionForImplicitMethodInvocationShouldReturnCorrectlyWhenMethodIsAuthorized(
      String attributeName) throws NameResolutionException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);

    Object result = attributeDescriptor.readReflection(testBean, queryExecutionContext);
    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo(attributeName);
  }

  @Test
  @Parameters({
      "nonPublicAttributeWithPublicAccessor, nonPublicAttributeWithPublicAccessor",
      "nonPublicAttributeWithPublicGetterMethod, getNonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldReturnCorrectlyWhenMethodIsAuthorizedAndCacheResult(
      String attributeName, String methodName) throws Exception {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);
    attributeDescriptor.readReflection(testBean, queryExecutionContext);

    Method method = TestBean.class.getMethod(methodName);
    assertThat(queryExecutionContext.cacheGet(method)).isEqualTo(true);
  }

  @Test
  @Parameters({
      "nonPublicAttributeWithPublicAccessor, nonPublicAttributeWithPublicAccessor",
      "nonPublicAttributeWithPublicGetterMethod, getNonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldReturnCorrectlyWhenMethodIsAuthorizedAndDoNotCacheResultForCqs(
      String attributeName, String methodName) throws Exception {
    when(queryExecutionContext.isCqQueryContext()).thenReturn(true);
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);
    attributeDescriptor.readReflection(testBean, queryExecutionContext);

    Method method = TestBean.class.getMethod(methodName);
    assertThat(queryExecutionContext.cacheGet(method)).isNull();
  }

  @Test
  public void readReflectionForImplicitMethodInvocationShouldUseCachedAuthorizerResultWhenMethodIsAuthorizedAndQueryContextIsTheSame() {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor nonPublicAttributeWithPublicAccessor =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_NAME);
    AttributeDescriptor nonPublicAttributeWithPublicGetterMethod =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_GETTER);

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        assertThat(
            nonPublicAttributeWithPublicAccessor.readReflection(testBean, queryExecutionContext))
                .isInstanceOf(String.class).isEqualTo(PRIVATE_ACCESSOR_BY_NAME);
        assertThat(nonPublicAttributeWithPublicGetterMethod.readReflection(testBean,
            queryExecutionContext)).isInstanceOf(String.class)
                .isEqualTo(PRIVATE_ACCESSOR_BY_GETTER);
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(2)).authorize(any(), any());
  }

  @Test
  public void readReflectionForImplicitMethodInvocationShouldNotUseCachedAuthorizerResultWhenMethodIsAuthorizedAndQueryContextIsNotTheSame() {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor nonPublicAttributeWithPublicAccessor =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_NAME);
    AttributeDescriptor nonPublicAttributeWithPublicGetterMethod =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_GETTER);

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        QueryExecutionContext mockContext = mock(QueryExecutionContext.class);
        when(mockContext.getMethodInvocationAuthorizer()).thenReturn(methodInvocationAuthorizer);

        assertThat(nonPublicAttributeWithPublicAccessor.readReflection(testBean, mockContext))
            .isInstanceOf(String.class).isEqualTo(PRIVATE_ACCESSOR_BY_NAME);
        assertThat(nonPublicAttributeWithPublicGetterMethod.readReflection(testBean, mockContext))
            .isInstanceOf(String.class).isEqualTo(PRIVATE_ACCESSOR_BY_GETTER);
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(20)).authorize(any(), any());
  }

  @Test
  public void readReflectionForImplicitMethodInvocationShouldNotUseCachedAuthorizerResultForCqs() {
    when(queryExecutionContext.isCqQueryContext()).thenReturn(true);
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor nonPublicAttributeWithPublicAccessor =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_NAME);
    AttributeDescriptor nonPublicAttributeWithPublicGetterMethod =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_GETTER);

    // Same QueryExecutionContext but CQ -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        assertThat(
            nonPublicAttributeWithPublicAccessor.readReflection(testBean, queryExecutionContext))
                .isInstanceOf(String.class).isEqualTo(PRIVATE_ACCESSOR_BY_NAME);
        assertThat(nonPublicAttributeWithPublicGetterMethod.readReflection(testBean,
            queryExecutionContext)).isInstanceOf(String.class)
                .isEqualTo(PRIVATE_ACCESSOR_BY_GETTER);
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(20)).authorize(any(), any());
  }

  @Test
  @Parameters({PRIVATE_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_GETTER})
  public void readReflectionForImplicitMethodInvocationShouldThrowExceptionWhenMethodIsNotAuthorized(
      String attributeName) {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);

    assertThatThrownBy(() -> attributeDescriptor.readReflection(testBean, queryExecutionContext))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
  }

  @Test
  @Parameters({
      "nonPublicAttributeWithPublicAccessor, nonPublicAttributeWithPublicAccessor",
      "nonPublicAttributeWithPublicGetterMethod, getNonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldThrowExceptionWhenMethodIsNotAuthorizedAndCacheResult(
      String attributeName, String methodName) throws NoSuchMethodException {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);

    assertThatThrownBy(() -> attributeDescriptor.readReflection(testBean, queryExecutionContext))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);

    Method method = TestBean.class.getMethod(methodName);
    assertThat(queryExecutionContext.cacheGet(method)).isEqualTo(false);
  }

  @Test
  @Parameters({
      "nonPublicAttributeWithPublicAccessor, nonPublicAttributeWithPublicAccessor",
      "nonPublicAttributeWithPublicGetterMethod, getNonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldThrowExceptionWhenMethodIsNotAuthorizedAndDoNotCacheResultForCqs(
      String attributeName, String methodName) throws NoSuchMethodException {
    when(queryExecutionContext.isCqQueryContext()).thenReturn(true);
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);

    assertThatThrownBy(() -> attributeDescriptor.readReflection(testBean, queryExecutionContext))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);

    Method method = TestBean.class.getMethod(methodName);
    assertThat(queryExecutionContext.cacheGet(method)).isNull();
  }

  @Test
  public void readReflectionForImplicitMethodInvocationShouldUseCachedAuthorizerResultWhenMethodIsForbiddenAndQueryContextIsTheSame() {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor nonPublicAttributeWithPublicAccessor =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_NAME);
    AttributeDescriptor nonPublicAttributeWithPublicGetterMethod =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_GETTER);

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> {
      assertThatThrownBy(() -> nonPublicAttributeWithPublicAccessor.readReflection(testBean,
          queryExecutionContext)).isInstanceOf(NotAuthorizedException.class)
              .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
      assertThatThrownBy(() -> nonPublicAttributeWithPublicGetterMethod.readReflection(testBean,
          queryExecutionContext)).isInstanceOf(NotAuthorizedException.class)
              .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
    });
    verify(methodInvocationAuthorizer, times(2)).authorize(any(), any());
  }

  @Test
  public void readReflectionForImplicitMethodInvocationShouldNotUseCachedAuthorizerResultWhenMethodIsForbiddenAndQueryContextIsNotTheSame() {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor nonPublicAttributeWithPublicAccessor =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_NAME);
    AttributeDescriptor nonPublicAttributeWithPublicGetterMethod =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_GETTER);

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      QueryExecutionContext mockContext = mock(QueryExecutionContext.class);
      when(mockContext.getMethodInvocationAuthorizer()).thenReturn(methodInvocationAuthorizer);

      assertThatThrownBy(
          () -> nonPublicAttributeWithPublicAccessor.readReflection(testBean, mockContext))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
      assertThatThrownBy(
          () -> nonPublicAttributeWithPublicGetterMethod.readReflection(testBean, mockContext))
              .isInstanceOf(NotAuthorizedException.class)
              .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
    });
    verify(methodInvocationAuthorizer, times(20)).authorize(any(), any());
  }

  @Test
  public void readReflectionForImplicitMethodInvocationShouldNotUseCachedAuthorizerResultWhenMethodIsForbiddenForCqs() {
    when(queryExecutionContext.isCqQueryContext()).thenReturn(true);
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor nonPublicAttributeWithPublicAccessor =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_NAME);
    AttributeDescriptor nonPublicAttributeWithPublicGetterMethod =
        new AttributeDescriptor(typeRegistry, PRIVATE_ACCESSOR_BY_GETTER);

    // Same QueryExecutionContext but CQ -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      assertThatThrownBy(() -> nonPublicAttributeWithPublicAccessor.readReflection(testBean,
          queryExecutionContext)).isInstanceOf(NotAuthorizedException.class)
              .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
      assertThatThrownBy(() -> nonPublicAttributeWithPublicGetterMethod.readReflection(testBean,
          queryExecutionContext)).isInstanceOf(NotAuthorizedException.class)
              .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
    });
    verify(methodInvocationAuthorizer, times(20)).authorize(any(), any());
  }

  @Test
  public void readShouldReturnUndefinedForNullOrUndefinedTargetObject()
      throws NameNotFoundException, QueryInvocationTargetException {
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, "whatever");
    assertThat(attributeDescriptor.read(null, queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
    assertThat(attributeDescriptor.read(QueryService.UNDEFINED, queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
  }

  @Test
  @Parameters({PUBLIC_NO_ACCESSORS, PUBLIC_ACCESSOR_BY_NAME, PUBLIC_ACCESSOR_BY_GETTER,
      PRIVATE_ACCESSOR_BY_NAME, PRIVATE_ACCESSOR_BY_GETTER})
  public void readShouldReturnCorrectlyForAccessibleAuthorizedNonPdxMembers(String attributeName)
      throws NameNotFoundException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = new AttributeDescriptor(typeRegistry, attributeName);

    Object result = attributeDescriptor.read(testBean, queryExecutionContext);
    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo(attributeName);
  }

  @SuppressWarnings("unused")
  private static class TestBean {
    public final String publicAttributeWithoutAccessors;
    public final String publicAttributeWithPublicAccessor;
    public final String publicAttributeWithPublicGetterMethod;
    private final String nonPublicAttributeWithPublicAccessor;
    private final String nonPublicAttributeWithPublicGetterMethod;

    public String publicAttributeWithPublicAccessor() {
      return publicAttributeWithPublicAccessor;
    }

    public String getPublicAttributeWithPublicGetterMethod() {
      return publicAttributeWithPublicGetterMethod;
    }

    public String nonPublicAttributeWithPublicAccessor() {
      return nonPublicAttributeWithPublicAccessor;
    }

    public String getNonPublicAttributeWithPublicGetterMethod() {
      return nonPublicAttributeWithPublicGetterMethod;
    }

    public void throwEntryDestroyedExceptionMethod() {
      throw new EntryDestroyedException();
    }

    TestBean(String publicAttributeWithoutAccessors,
        String publicAttributeWithPublicAccessor,
        String publicAttributeWithPublicGetterMethod,
        String nonPublicAttributeWithPublicAccessor,
        String nonPublicAttributeWithPublicGetterMethod) {
      this.publicAttributeWithoutAccessors = publicAttributeWithoutAccessors;
      this.publicAttributeWithPublicAccessor = publicAttributeWithPublicAccessor;
      this.publicAttributeWithPublicGetterMethod = publicAttributeWithPublicGetterMethod;
      this.nonPublicAttributeWithPublicAccessor = nonPublicAttributeWithPublicAccessor;
      this.nonPublicAttributeWithPublicGetterMethod = nonPublicAttributeWithPublicGetterMethod;
    }
  }
}
