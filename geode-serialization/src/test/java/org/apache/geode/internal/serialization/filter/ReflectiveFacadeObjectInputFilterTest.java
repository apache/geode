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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ReflectiveFacadeObjectInputFilterTest {

  private ObjectInputFilterApi api;
  private ObjectInputStream objectInputStream;

  @Before
  public void setUp() {
    api = mock(ObjectInputFilterApi.class);
    objectInputStream = mock(ObjectInputStream.class);
  }

  @Test
  public void createsObjectInputFilterProxy()
      throws InvocationTargetException, IllegalAccessException, UnableToSetSerialFilterException {
    String pattern = "the-pattern";
    Collection<String> sanctionedClasses = asList("class-name-one", "class-name-two");
    StreamSerialFilter objectInputFilter =
        new ReflectiveFacadeStreamSerialFilter(api, pattern, sanctionedClasses);

    objectInputFilter.setFilterOn(objectInputStream);

    ArgumentCaptor<Collection<String>> captor = uncheckedCast(forClass(Collection.class));
    verify(api).createObjectInputFilterProxy(eq(pattern), captor.capture());
    assertThat(captor.getValue()).containsExactlyElementsOf(sanctionedClasses);
  }

  @Test
  public void setsSerialFilter()
      throws InvocationTargetException, IllegalAccessException, UnableToSetSerialFilterException {
    StreamSerialFilter objectInputFilter =
        new ReflectiveFacadeStreamSerialFilter(api, "the-pattern", singleton("class-name"));

    objectInputFilter.setFilterOn(objectInputStream);

    verify(api).setObjectInputFilter(same(objectInputStream), any());
  }

  @Test
  public void propagatesIllegalAccessExceptionInUnsupportedOperationException()
      throws InvocationTargetException, IllegalAccessException {
    IllegalAccessException illegalAccessException = new IllegalAccessException("testing");
    doThrow(illegalAccessException).when(api).setObjectInputFilter(same(objectInputStream), any());
    StreamSerialFilter objectInputFilter =
        new ReflectiveFacadeStreamSerialFilter(api, "the-pattern", singleton("class-name"));

    Throwable thrown = catchThrowable(() -> {
      objectInputFilter.setFilterOn(objectInputStream);
    });

    assertThat(thrown)
        .isInstanceOf(UnableToSetSerialFilterException.class)
        .hasMessage("Unable to configure an input stream serialization filter using reflection.")
        .hasRootCause(illegalAccessException);
  }

  @Test
  public void propagatesInvocationTargetExceptionInUnsupportedOperationException()
      throws InvocationTargetException, IllegalAccessException {
    InvocationTargetException invocationTargetException =
        new InvocationTargetException(new Exception("testing"), "testing");
    doThrow(invocationTargetException).when(api).setObjectInputFilter(same(objectInputStream),
        any());
    StreamSerialFilter objectInputFilter =
        new ReflectiveFacadeStreamSerialFilter(api, "the-pattern", singleton("class-name"));

    Throwable thrown = catchThrowable(() -> {
      objectInputFilter.setFilterOn(objectInputStream);
    });

    assertThat(thrown)
        .isInstanceOf(UnableToSetSerialFilterException.class)
        .hasMessage(
            "Unable to configure an input stream serialization filter because invocation target threw "
                + Exception.class.getName() + ".")
        .hasCause(invocationTargetException);
  }

  /**
   * The ObjectInputFilter API throws IllegalStateException nested within InvocationTargetException
   * if a non-null filter already exists.
   */
  @Test
  public void propagatesNestedIllegalStateExceptionInObjectInputFilterException()
      throws InvocationTargetException, IllegalAccessException {
    StreamSerialFilter objectInputFilter =
        new ReflectiveFacadeStreamSerialFilter(api, "the-pattern", singleton("class-name"));
    InvocationTargetException invocationTargetException =
        new InvocationTargetException(new IllegalStateException("testing"), "testing");
    doThrow(invocationTargetException)
        .when(api).setObjectInputFilter(same(objectInputStream), any());

    Throwable thrown = catchThrowable(() -> {
      objectInputFilter.setFilterOn(objectInputStream);
    });

    assertThat(thrown)
        .isInstanceOf(FilterAlreadyConfiguredException.class)
        .hasMessage(
            "Unable to configure an input stream serialization filter because a non-null filter has already been set.")
        .hasCauseInstanceOf(InvocationTargetException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class)
        .hasRootCauseMessage("testing");
  }

  @Test
  public void delegatesToObjectInputFilterApiToCreateObjectInputFilter()
      throws InvocationTargetException, IllegalAccessException, UnableToSetSerialFilterException {
    ObjectInputFilterApi api = mock(ObjectInputFilterApi.class);
    StreamSerialFilter filter = new ReflectiveFacadeStreamSerialFilter(api, "pattern", emptySet());
    Object objectInputFilter = mock(Object.class);
    ObjectInputStream objectInputStream = mock(ObjectInputStream.class);

    when(api.createObjectInputFilterProxy(any(), any()))
        .thenReturn(objectInputFilter);

    filter.setFilterOn(objectInputStream);

    verify(api).createObjectInputFilterProxy(any(), any());
  }

  @Test
  public void delegatesToObjectInputFilterApiToSetFilterOnObjectInputStream()
      throws InvocationTargetException, IllegalAccessException, UnableToSetSerialFilterException {
    ObjectInputFilterApi api = mock(ObjectInputFilterApi.class);
    StreamSerialFilter filter = new ReflectiveFacadeStreamSerialFilter(api, "pattern", emptySet());
    Object objectInputFilter = mock(Object.class);
    ObjectInputStream objectInputStream = mock(ObjectInputStream.class);

    when(api.createObjectInputFilterProxy(any(), any()))
        .thenReturn(objectInputFilter);

    filter.setFilterOn(objectInputStream);

    verify(api).setObjectInputFilter(objectInputStream, objectInputFilter);
  }
}
