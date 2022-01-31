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
      throws InvocationTargetException, IllegalAccessException, UnableToSetSerialFilterException {
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
  public void setsSerialFilter()
      throws InvocationTargetException, IllegalAccessException, UnableToSetSerialFilterException {
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, "the-pattern", singleton("class-name"));
    when(api.createObjectInputFilterProxy(eq("the-pattern"), anyCollection()))
        .thenReturn(objectInputFilter);

    globalSerialFilter.setFilter();

    verify(api).setSerialFilter(any());
  }

  @Test
  public void propagatesIllegalAccessExceptionInObjectInputFilterException()
      throws InvocationTargetException, IllegalAccessException {
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, "the-pattern", singleton("class-name"));
    IllegalAccessException illegalAccessException = new IllegalAccessException("testing");
    when(api.createObjectInputFilterProxy(eq("the-pattern"), anyCollection()))
        .thenReturn(objectInputFilter);
    doThrow(illegalAccessException)
        .when(api).setSerialFilter(any());

    Throwable thrown = catchThrowable(() -> {
      globalSerialFilter.setFilter();
    });

    assertThat(thrown)
        .isInstanceOf(UnableToSetSerialFilterException.class)
        .hasMessage("Unable to configure a global serialization filter using reflection.")
        .hasRootCause(illegalAccessException)
        .hasRootCauseMessage("testing");
  }

  @Test
  public void propagatesInvocationTargetExceptionInObjectInputFilterException()
      throws InvocationTargetException, IllegalAccessException {
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, "the-pattern", singleton("class-name"));
    InvocationTargetException invocationTargetException =
        new InvocationTargetException(new Exception("testing"), "testing");
    doThrow(invocationTargetException).when(api).setSerialFilter(any());

    Throwable thrown = catchThrowable(() -> {
      globalSerialFilter.setFilter();
    });

    assertThat(thrown)
        .isInstanceOf(UnableToSetSerialFilterException.class)
        .hasMessage(
            "Unable to configure a global serialization filter because invocation target threw "
                + Exception.class.getName() + ".")
        .hasCause(invocationTargetException)
        .hasRootCauseInstanceOf(Exception.class)
        .hasRootCauseMessage("testing");
  }

  /**
   * The ObjectInputFilter API throws IllegalStateException nested within InvocationTargetException
   * if a non-null filter already exists.
   */
  @Test
  public void propagatesNestedIllegalStateExceptionInObjectInputFilterException()
      throws InvocationTargetException, IllegalAccessException {
    GlobalSerialFilter globalSerialFilter =
        new ReflectiveFacadeGlobalSerialFilter(api, "the-pattern", singleton("class-name"));
    InvocationTargetException invocationTargetException =
        new InvocationTargetException(new IllegalStateException("testing"), "testing");
    doThrow(invocationTargetException).when(api).setSerialFilter(any());

    Throwable thrown = catchThrowable(() -> {
      globalSerialFilter.setFilter();
    });

    assertThat(thrown)
        .isInstanceOf(FilterAlreadyConfiguredException.class)
        .hasMessage(
            "Unable to configure a global serialization filter because filter has already been set non-null.")
        .hasCauseInstanceOf(InvocationTargetException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class)
        .hasRootCauseMessage("testing");
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
