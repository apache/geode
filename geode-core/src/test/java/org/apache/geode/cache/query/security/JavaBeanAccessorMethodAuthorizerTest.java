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

import java.io.File;
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
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class JavaBeanAccessorMethodAuthorizerTest {
  private InternalCache mockCache;
  private JavaBeanAccessorMethodAuthorizer authorizerWithLangAndIOPackagesSpecified;
  private RestrictedMethodAuthorizer defaultAuthorizer;
  private final String LANG_PACKAGE = String.class.getPackage().getName();
  private final String IO_PACKAGE = File.class.getPackage().getName();

  @Before
  public void setUp() {
    mockCache = mock(InternalCache.class);
    defaultAuthorizer = new RestrictedMethodAuthorizer(mockCache);

    Set<String> allowedPackages = new HashSet<>();
    allowedPackages.add(LANG_PACKAGE);
    allowedPackages.add(IO_PACKAGE);

    authorizerWithLangAndIOPackagesSpecified =
        new JavaBeanAccessorMethodAuthorizer(defaultAuthorizer, allowedPackages);
  }

  @Test
  public void constructorThrowsExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> new JavaBeanAccessorMethodAuthorizer((Cache) null, new HashSet<>()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_CACHE_MESSAGE);
  }

  @Test
  public void constructorThrowsExceptionWhenRestrictedMethodAuthorizerIsNull() {
    assertThatThrownBy(() -> new JavaBeanAccessorMethodAuthorizer((RestrictedMethodAuthorizer) null,
        new HashSet<>()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_AUTHORIZER_MESSAGE);
  }

  @Test
  public void constructorsThrowsExceptionWhenAllowedPackagesIsNull() {
    assertThatThrownBy(() -> new JavaBeanAccessorMethodAuthorizer(mockCache, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_PACKAGE_MESSAGE);

    assertThatThrownBy(
        () -> new JavaBeanAccessorMethodAuthorizer(defaultAuthorizer, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage(JavaBeanAccessorMethodAuthorizer.NULL_PACKAGE_MESSAGE);
  }

  @Test
  public void authorizeReturnsFalseForKnownDangerousMethods() throws NoSuchMethodException {
    List<Method> dangerousMethods = new ArrayList<>();
    dangerousMethods.add(TestBean.class.getMethod("getClass"));
    dangerousMethods.add(TestBean.class.getMethod("readResolve"));
    dangerousMethods.add(TestBean.class.getMethod("readObjectNoData"));
    dangerousMethods.add(TestBean.class.getMethod("readObject", ObjectInputStream.class));
    dangerousMethods.add(TestBean.class.getMethod("writeReplace"));
    dangerousMethods.add(TestBean.class.getMethod("writeObject", ObjectOutputStream.class));

    dangerousMethods.forEach(
        method -> assertThat(
            authorizerWithLangAndIOPackagesSpecified.authorize(method, new TestBean()))
                .isFalse());
  }

  @Test
  public void authorizeReturnsFalseForDisallowedGeodeClassesWithGeodePackageSpecified()
      throws NoSuchMethodException {
    assertThat((TestBean.class.getPackage().getName()))
        .startsWith(JavaBeanAccessorMethodAuthorizer.GEODE_BASE_PACKAGE);

    List<Method> geodeMethods = new ArrayList<>();
    geodeMethods.add(TestBean.class.getMethod("isMatchingMethod"));
    geodeMethods.add(TestBean.class.getMethod("getMatchingMethod"));
    geodeMethods.add(TestBean.class.getMethod("nonMatchingMethod"));

    Set<String> geodePackage = new HashSet<>();
    geodePackage.add(JavaBeanAccessorMethodAuthorizer.GEODE_BASE_PACKAGE);
    JavaBeanAccessorMethodAuthorizer geodeMatchingAuthorizer =
        new JavaBeanAccessorMethodAuthorizer(defaultAuthorizer, geodePackage);

    geodeMethods.forEach(
        method -> assertThat(geodeMatchingAuthorizer.authorize(method, new TestBean())).isFalse());
  }

  @Test
  public void authorizeReturnsFalseForMatchingMethodNamesAndNonMatchingPackage()
      throws NoSuchMethodException {

    Method getMatchingMethod = List.class.getMethod("get", int.class);
    Method isMatchingMethod = List.class.getMethod("isEmpty");

    assertThat(
        authorizerWithLangAndIOPackagesSpecified.authorize(isMatchingMethod, new ArrayList()))
            .isFalse();
    assertThat(
        authorizerWithLangAndIOPackagesSpecified.authorize(getMatchingMethod, new ArrayList()))
            .isFalse();
  }

  @Test
  public void authorizeReturnsFalseForNonMatchingMethodNameAndMatchingPackage()
      throws NoSuchMethodException {

    Method langMethod = String.class.getMethod("notify");
    assertThat(authorizerWithLangAndIOPackagesSpecified.authorize(langMethod, "")).isFalse();

    Method ioMethod = File.class.getMethod("notify");
    assertThat(authorizerWithLangAndIOPackagesSpecified.authorize(ioMethod, new File("")))
        .isFalse();
  }

  @Test
  public void authorizeReturnsTrueForMatchingMethodNamesAndPackage() throws NoSuchMethodException {
    Method isMatchingLangMethod = String.class.getMethod("isEmpty");
    Method getMatchingLangMethod = String.class.getMethod("getBytes");

    assertThat(authorizerWithLangAndIOPackagesSpecified.authorize(isMatchingLangMethod, ""))
        .isTrue();
    assertThat(authorizerWithLangAndIOPackagesSpecified.authorize(getMatchingLangMethod, ""))
        .isTrue();

    Method isMatchingIOMethod = File.class.getMethod("isAbsolute");
    Method getMatchingIOMethod = File.class.getMethod("getPath");

    assertThat(authorizerWithLangAndIOPackagesSpecified.authorize(isMatchingIOMethod, new File("")))
        .isTrue();
    assertThat(
        authorizerWithLangAndIOPackagesSpecified.authorize(getMatchingIOMethod, new File("")))
            .isTrue();
  }

  @Test
  public void authorizeReturnsFalseForNonMatchingDisallowedMethod() throws NoSuchMethodException {
    Method method = Object.class.getMethod("notify");

    assertThat(authorizerWithLangAndIOPackagesSpecified.authorize(method, new Object())).isFalse();
  }

  @Test
  public void authorizeReturnsTrueForNonMatchingAllowedMethod() throws NoSuchMethodException {
    Method method = Object.class.getMethod("equals", Object.class);

    assertThat(authorizerWithLangAndIOPackagesSpecified.authorize(method, new Object())).isTrue();
  }

  @Test
  public void allowedPackagesIsUnmodifiable() {
    assertThatThrownBy(
        () -> authorizerWithLangAndIOPackagesSpecified.getParameters().remove(LANG_PACKAGE))
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

    public void isMatchingMethod() {}

    public void getMatchingMethod() {}

    public void nonMatchingMethod() {}
  }

}
