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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
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

@RunWith(JUnitParamsRunner.class)
public class AttributeDescriptorTest {
  private TestBean testBean;
  private TypeRegistry typeRegistry;
  private QueryExecutionContext queryExecutionContext;
  private MethodInvocationAuthorizer methodInvocationAuthorizer;

  @Before
  public void setUp() {
    AttributeDescriptor._localCache.clear();
    testBean = new TestBean("publicAttributeWithoutAccessors", "publicAttributeWithPublicAccessor",
        "publicAttributeWithPublicGetterMethod", "nonPublicAttributeWithPublicAccessor",
        "nonPublicAttributeWithPublicGetterMethod");
    InternalCache mockCache = mock(InternalCache.class);
    typeRegistry = new TypeRegistry(mockCache, true);
    methodInvocationAuthorizer = spy(MethodInvocationAuthorizer.class);
    queryExecutionContext = new QueryExecutionContext(null, mockCache);
  }

  @Test
  @Parameters({"publicAttributeWithoutAccessors", "publicAttributeWithPublicAccessor",
      "publicAttributeWithPublicGetterMethod", "nonPublicAttributeWithPublicAccessor",
      "nonPublicAttributeWithPublicAccessor"})
  public void validateReadTypeShouldReturnTrueWhenMemberCanBeFound(String attributeName) {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);
    assertThat(attributeDescriptor.validateReadType(TestBean.class)).isTrue();
  }

  @Test
  public void validateReadTypeShouldReturnFalseWhenMemberCanNotBeFound() {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, "nonExistingAttribute");
    assertThat(attributeDescriptor.validateReadType(TestBean.class)).isFalse();
  }

  @Test
  @Parameters({"nonPublicAttributeWithPublicAccessor", "nonPublicAttributeWithPublicAccessor"})
  public void getReadFieldShouldReturnNullForNonPublicAttributes(String attributeName) {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);
    assertThat(attributeDescriptor.getReadField(TestBean.class)).isNull();
  }

  @Test
  @Parameters({"publicAttributeWithoutAccessors", "publicAttributeWithPublicAccessor",
      "publicAttributeWithPublicGetterMethod"})
  public void getReadFieldShouldReturnRequestedFieldForPublicAttributes(String attributeName)
      throws NoSuchFieldException {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);
    assertThat(attributeDescriptor.getReadField(TestBean.class))
        .isEqualTo(TestBean.class.getField(attributeName));
  }

  @Test
  @Parameters({"publicAttributeWithPublicGetterMethod", "nonPublicAttributeWithPublicGetterMethod"})
  public void getReadMethodShouldReturnRequestedMethodForAttributesWithGetters(String attributeName)
      throws NoSuchMethodException {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);
    String getterName =
        "get" + attributeName.substring(0, 1).toUpperCase() + attributeName.substring(1);
    assertThat(attributeDescriptor.getReadMethod(TestBean.class))
        .isEqualTo(TestBean.class.getMethod(getterName));
  }

  @Test
  @Parameters({"publicAttributeWithPublicAccessor", "nonPublicAttributeWithPublicAccessor"})
  public void getReadMethodShouldReturnRequestedMethodForAttributesWithAccessors(
      String attributeName) throws NoSuchMethodException {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);
    assertThat(attributeDescriptor.getReadMethod(TestBean.class, attributeName))
        .isEqualTo(TestBean.class.getMethod(attributeName));
  }

  @Test
  public void getReadMethodShouldReturnNullAndUpdateCachedClassToMethodsMapWhenMethodCanNotBeFound() {
    DefaultQuery.getPdxClasstoMethodsmap().clear();
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, "nonExistingAttribute");
    assertThat(attributeDescriptor.getReadMethod(TestBean.class)).isNull();
    assertThat(DefaultQuery.getPdxClasstoMethodsmap().isEmpty()).isFalse();
    assertThat(
        DefaultQuery.getPdxClasstoMethodsmap().containsKey(TestBean.class.getCanonicalName()))
            .isTrue();
    assertThat(DefaultQuery.getPdxClasstoMethodsmap().get(TestBean.class.getCanonicalName())
        .contains("nonExistingAttribute")).isTrue();
  }

  @Test
  @Parameters({"publicAttributeWithoutAccessors", "publicAttributeWithPublicAccessor",
      "publicAttributeWithPublicGetterMethod"})
  public void getReadMemberShouldReturnFieldWhenAttributeIsPublicAndUseInternalCache(
      String attributeName) throws NameNotFoundException {
    AttributeDescriptor attributeDescriptor =
        spy(new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName));
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Field.class);
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Field.class);

    // Second time the field should be obtained from the local cache.
    verify(attributeDescriptor, times(1)).getReadField(TestBean.class);
  }

  @Test
  @Parameters({"nonPublicAttributeWithPublicAccessor", "nonPublicAttributeWithPublicGetterMethod"})
  public void getReadMemberShouldReturnMethodWhenAttributeIsNotPublicAndUseInternalCache(
      String attributeName) throws NameNotFoundException {
    AttributeDescriptor attributeDescriptor =
        spy(new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName));
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Method.class);
    assertThat(attributeDescriptor.getReadMember(TestBean.class)).isInstanceOf(Method.class);

    // Second time the field should be obtained from the local cache.
    verify(attributeDescriptor, times(1)).getReadMethod(TestBean.class);
  }

  @Test
  public void getReadMemberShouldThrowExceptionWhenMethodCanNotBeFound() {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, "nonExistingAttribute");
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
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, "nonExistingAttribute");
    assertThat(attributeDescriptor.readReflection(mock(Token.class), queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
  }

  @Test
  public void readReflectionShouldThrowExceptionWhenMemberCanNotBeFound() {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, "nonExistingAttribute");
    assertThatThrownBy(
        () -> attributeDescriptor.readReflection(TestBean.class, queryExecutionContext))
            .isInstanceOf(NameNotFoundException.class);
  }

  @Test
  public void readReflectionShouldReturnUndefinedWhenEntryDestroyedExceptionIsThrown()
      throws NameNotFoundException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor = spy(new AttributeDescriptor(typeRegistry,
        methodInvocationAuthorizer, "throwEntryDestroyedExceptionMethod"));
    assertThat(attributeDescriptor.readReflection(testBean, queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
  }

  @Test
  @Parameters({"publicAttributeWithoutAccessors", "publicAttributeWithPublicAccessor",
      "publicAttributeWithPublicGetterMethod"})
  public void readReflectionForPublicAttributeShouldNotInvokeAuthorize(String attributeName)
      throws NameResolutionException, QueryInvocationTargetException {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);

    Object result = attributeDescriptor.readReflection(testBean, queryExecutionContext);
    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo(attributeName);
    verify(methodInvocationAuthorizer, never()).authorize(any(), any());
  }

  @Test
  @Parameters({"nonPublicAttributeWithPublicAccessor", "nonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldReturnCorrectlyWhenMethodIsAuthorized(
      String attributeName) throws NameResolutionException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);

    Object result = attributeDescriptor.readReflection(testBean, queryExecutionContext);
    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo(attributeName);
  }

  @Test
  @Parameters({"nonPublicAttributeWithPublicAccessor", "nonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldUseAlreadyAuthorizedCachedResultWhenMethodIsAuthorized(
      String attributeName) {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        Object result = attributeDescriptor.readReflection(testBean, queryExecutionContext);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo(attributeName);
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(1)).authorize(any(), any());

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> {
      try {
        QueryExecutionContext mockContext = mock(QueryExecutionContext.class);
        Object result = attributeDescriptor.readReflection(testBean, mockContext);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo(attributeName);
      } catch (NameNotFoundException | QueryInvocationTargetException exception) {
        throw new RuntimeException(exception);
      }
    });
    verify(methodInvocationAuthorizer, times(11)).authorize(any(), any());
  }

  @Test
  @Parameters({"nonPublicAttributeWithPublicAccessor", "nonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldThrowExceptionWhenMethodIsNotAuthorized(
      String attributeName) {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);

    assertThatThrownBy(() -> attributeDescriptor.readReflection(testBean, queryExecutionContext))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
  }

  @Test
  @Parameters({"nonPublicAttributeWithPublicAccessor", "nonPublicAttributeWithPublicGetterMethod"})
  public void readReflectionForImplicitMethodInvocationShouldUseAlreadyAuthorizedCachedResultWhenMethodIsNotAuthorized(
      String attributeName) {
    doReturn(false).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);

    // Same QueryExecutionContext -> Use cache.
    IntStream.range(0, 10).forEach(element -> assertThatThrownBy(
        () -> attributeDescriptor.readReflection(testBean, queryExecutionContext))
            .isInstanceOf(NotAuthorizedException.class)
            .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING));
    verify(methodInvocationAuthorizer, times(1)).authorize(any(), any());

    // New QueryExecutionContext every time -> Do not use cache.
    IntStream.range(0, 10).forEach(element -> assertThatThrownBy(
        () -> attributeDescriptor.readReflection(testBean, mock(QueryExecutionContext.class)))
            .isInstanceOf(NotAuthorizedException.class)
            .hasMessageStartingWith(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING));
    verify(methodInvocationAuthorizer, times(11)).authorize(any(), any());
  }

  @Test
  public void readShouldReturnUndefinedForNullOrUndefinedTargetObject()
      throws NameNotFoundException, QueryInvocationTargetException {
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, "whatever");
    assertThat(attributeDescriptor.read(null, queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
    assertThat(attributeDescriptor.read(QueryService.UNDEFINED, queryExecutionContext))
        .isEqualTo(QueryService.UNDEFINED);
  }

  @Test
  @Parameters({"publicAttributeWithoutAccessors", "publicAttributeWithPublicAccessor",
      "publicAttributeWithPublicGetterMethod", "nonPublicAttributeWithPublicAccessor",
      "nonPublicAttributeWithPublicAccessor"})
  public void readShouldReturnCorrectlyForAccessibleAuthorizedNonPdxMembers(String attributeName)
      throws NameNotFoundException, QueryInvocationTargetException {
    doReturn(true).when(methodInvocationAuthorizer).authorize(any(), any());
    AttributeDescriptor attributeDescriptor =
        new AttributeDescriptor(typeRegistry, methodInvocationAuthorizer, attributeName);

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
