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
package org.apache.geode.cache.query.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class JavaBeanAccessorMethodAuthorizerTest {
  private InternalCache mockCache;
  private SecurityService mockSecurityService;
  private JavaBeanAccessorMethodAuthorizer authorizerWithNoPackages;
  private final String TEST_PACKAGE = this.getClass().getPackage().getName();
  private Set<String> allowedPackages;
  private JavaBeanAccessorMethodAuthorizer authorizerWithPackageSpecified;

  @Before
  public void setUp() {
    mockCache = mock(InternalCache.class);
    mockSecurityService = mock(SecurityService.class);
    when(mockCache.getSecurityService()).thenReturn(mockSecurityService);
    allowedPackages = new HashSet<>();
    allowedPackages.add(TEST_PACKAGE);
    authorizerWithNoPackages = new JavaBeanAccessorMethodAuthorizer(
        new RestrictedMethodAuthorizer(mockCache), new HashSet<>());
    authorizerWithPackageSpecified = new JavaBeanAccessorMethodAuthorizer(
        new RestrictedMethodAuthorizer(mockCache), allowedPackages);
  }

  @Test
  public void constructorShouldThrowExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> new JavaBeanAccessorMethodAuthorizer((Cache) null, new HashSet<>()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_CACHE_MESSAGE);
  }

  @Test
  public void constructorShouldThrowExceptionWhenRestrictedMethodAuthorizerIsNull() {
    assertThatThrownBy(() -> new JavaBeanAccessorMethodAuthorizer((RestrictedMethodAuthorizer) null,
        new HashSet<>()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_AUTHORIZER_MESSAGE);
  }

  @Test
  public void constructorsShouldThrowExceptionWhenAllowedPackagesIsNull() {
    assertThatThrownBy(() -> new JavaBeanAccessorMethodAuthorizer(mockCache, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_PACKAGE_MESSAGE);

    assertThatThrownBy(
        () -> new JavaBeanAccessorMethodAuthorizer(new RestrictedMethodAuthorizer(mockCache), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_PACKAGE_MESSAGE);
  }

  @Test
  public void authorizeShouldReturnFalseForKnownDangerousMethods() throws Exception {
    TestBean testBean = new TestBean();
    List<Method> dangerousMethods = new ArrayList<>();
    dangerousMethods.add(TestBean.class.getMethod("getClass"));
    dangerousMethods.add(TestBean.class.getMethod("readResolve"));
    dangerousMethods.add(TestBean.class.getMethod("readObjectNoData"));
    dangerousMethods.add(TestBean.class.getMethod("readObject", ObjectInputStream.class));
    dangerousMethods.add(TestBean.class.getMethod("writeReplace"));
    dangerousMethods.add(TestBean.class.getMethod("writeObject", ObjectOutputStream.class));

    dangerousMethods.forEach(
        method -> assertThat(authorizerWithNoPackages.authorize(method, testBean)).isFalse());
  }

  @Test
  public void authorizeReturnsFalseForNonMatchingDisallowedMethod() throws NoSuchMethodException {
    TestBean testBean = new TestBean();
    Method nonMatchingMethod = TestBean.class.getMethod("nonMatchingMethod");
    assertThat(authorizerWithNoPackages.authorize(nonMatchingMethod, testBean)).isFalse();
  }

  @Test
  public void authorizeReturnsFalseForNonMatchingMethodWithMatchingPackage()
      throws NoSuchMethodException {
    TestBean testBean = new TestBean();
    Method nonMatchingMethod = TestBean.class.getMethod("nonMatchingMethod");
    assertThat(authorizerWithPackageSpecified.authorize(nonMatchingMethod, testBean)).isFalse();
  }

  @Test
  public void authorizeReturnsTrueForNonMatchingAllowedMethod() throws NoSuchMethodException {
    Object object = new Object();
    Method method = Object.class.getMethod("equals", Object.class);
    assertThat(authorizerWithNoPackages.authorize(method, object)).isTrue();
  }

  @Test
  public void authorizeReturnsFalseForMatchingMethodNamesButNonMatchingPackage()
      throws NoSuchMethodException {
    TestBean testBean = new TestBean();

    Set<String> wrongPackages = new HashSet<>();
    wrongPackages.add("my.fake.package");
    JavaBeanAccessorMethodAuthorizer authorizer = new JavaBeanAccessorMethodAuthorizer(
        new RestrictedMethodAuthorizer(mockCache), wrongPackages);

    Method isMatchingMethod = TestBean.class.getMethod("isMatchingMethod");
    Method getMatchingMethod = TestBean.class.getMethod("getMatchingMethod");

    assertThat(authorizer.authorize(isMatchingMethod, testBean)).isFalse();
    assertThat(authorizer.authorize(getMatchingMethod, testBean)).isFalse();
  }

  @Test
  public void authorizeReturnsTrueForMatchingMethodNamesAndPackage() throws NoSuchMethodException {
    TestBean testBean = new TestBean();

    Method isMatchingMethod = TestBean.class.getMethod("isMatchingMethod");
    Method getMatchingMethod = TestBean.class.getMethod("getMatchingMethod");

    assertThat(authorizerWithPackageSpecified.authorize(isMatchingMethod, testBean)).isTrue();
    assertThat(authorizerWithPackageSpecified.authorize(getMatchingMethod, testBean)).isTrue();
  }

  @Test
  public void allowedPackagesIsUnmodifiable() {
    assertThatThrownBy(() -> authorizerWithNoPackages.getAllowedPackages().remove(TEST_PACKAGE))
        .isInstanceOf(UnsupportedOperationException.class);
  }


  @SuppressWarnings("unused")
  private static class TestBean implements Serializable {
    public Object writeReplace() throws ObjectStreamException {
      return new TestBean();
    }

    public void writeObject(ObjectOutputStream stream) throws IOException {
      throw new IOException();
    }

    public Object readResolve() throws ObjectStreamException {
      return new TestBean();
    }

    public void readObjectNoData() throws ObjectStreamException {}

    public void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
      if (new Random().nextBoolean()) {
        throw new IOException();
      } else {
        throw new ClassNotFoundException();
      }
    }

    public void nonMatchingMethod() {}

    public void isMatchingMethod() {}

    public void getMatchingMethod() {}
  }

}
