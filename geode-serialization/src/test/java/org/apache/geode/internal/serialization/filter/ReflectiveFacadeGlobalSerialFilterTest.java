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
package org.apache.geode.internal.serialization.filter;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ReflectiveFacadeGlobalSerialFilterTest {

  private ObjectInputFilterApi api;
  private Object objectInputFilter;

  @Before
  public void setUp() {
    api = mock(ObjectInputFilterApi.class);
    objectInputFilter = new Object();
  }

  @Test
  public void createsObjectInputFilterProxy()
      throws InvocationTargetException, IllegalAccessException {
    String pattern = "the-pattern";
    Collection<String> sanctionedClasses = asList("class-name-one", "class-name-two");
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, pattern, sanctionedClasses);
    when(api.createObjectInputFilterProxy(eq("the-pattern"), anyCollection()))
        .thenReturn(objectInputFilter);

    globalSerialFilter.setFilter();

    ArgumentCaptor<Collection<String>> captor = uncheckedCast(forClass(Collection.class));
    verify(api).createObjectInputFilterProxy(eq(pattern), captor.capture());
    assertThat(captor.getValue()).containsExactlyElementsOf(sanctionedClasses);
  }

  @Test
  public void setsSerialFilter() throws InvocationTargetException, IllegalAccessException {
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, "the-pattern", singleton("class-name"));
    when(api.createObjectInputFilterProxy(eq("the-pattern"), anyCollection()))
        .thenReturn(objectInputFilter);

    globalSerialFilter.setFilter();

    verify(api).setSerialFilter(any());
  }

  @Test
  public void propagatesIllegalAccessExceptionInUnsupportedOperationException()
      throws InvocationTargetException, IllegalAccessException {
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, "the-pattern", singleton("class-name"));
    IllegalAccessException exception = new IllegalAccessException("testing");
    when(api.createObjectInputFilterProxy(eq("the-pattern"), anyCollection()))
        .thenReturn(objectInputFilter);
    doThrow(exception)
        .when(api).setSerialFilter(any());

    Throwable thrown = catchThrowable(() -> {
      globalSerialFilter.setFilter();
    });

    assertThat(thrown)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasRootCause(exception);
  }

  @Test
  public void propagatesInvocationTargetExceptionInUnsupportedOperationException()
      throws InvocationTargetException, IllegalAccessException {
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, "the-pattern", singleton("class-name"));
    InvocationTargetException exception =
        new InvocationTargetException(new Exception("testing"), "testing");
    doThrow(exception).when(api).setSerialFilter(any());

    Throwable thrown = catchThrowable(() -> {
      globalSerialFilter.setFilter();
    });

    assertThat(thrown)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasCause(exception);
  }

  @Test
  public void requiresObjectInputFilterApi() {
    Throwable thrown = catchThrowable(() -> {
      new ReflectiveFacadeGlobalSerialFilter(null, "pattern", emptySet());
    });

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class)
        .hasMessage("ObjectInputFilterApi is required");
  }
}
