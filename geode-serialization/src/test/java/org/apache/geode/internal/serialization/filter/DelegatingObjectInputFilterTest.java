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

import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class DelegatingObjectInputFilterTest {

  private ObjectInputFilterApi api;
  private ObjectInputStream objectInputStream;

  @Before
  public void setUp() {
    api = mock(ObjectInputFilterApi.class);
    objectInputStream = mock(ObjectInputStream.class);
  }

  @Test
  public void createsObjectInputFilterProxy()
      throws InvocationTargetException, IllegalAccessException {
    String pattern = "the-pattern";
    Collection<String> sanctionedClasses = asList("class-name-one", "class-name-two");
    ObjectInputFilter objectInputFilter =
        new DelegatingObjectInputFilter(api, pattern, sanctionedClasses);

    objectInputFilter.setFilterOn(objectInputStream);

    ArgumentCaptor<Collection<String>> captor = uncheckedCast(forClass(Collection.class));
    verify(api).createObjectInputFilterProxy(eq(pattern), captor.capture());
    assertThat(captor.getValue()).containsExactlyElementsOf(sanctionedClasses);
  }

  @Test
  public void setsSerialFilter() throws InvocationTargetException, IllegalAccessException {
    ObjectInputFilter objectInputFilter =
        new DelegatingObjectInputFilter(api, "the-pattern", singleton("class-name"));

    objectInputFilter.setFilterOn(objectInputStream);

    verify(api).setObjectInputFilter(same(objectInputStream), any());
  }

  @Test
  public void propagatesIllegalAccessExceptionInUnsupportedOperationException()
      throws InvocationTargetException, IllegalAccessException {
    IllegalAccessException exception = new IllegalAccessException("testing");
    doThrow(exception).when(api).setObjectInputFilter(same(objectInputStream), any());
    ObjectInputFilter objectInputFilter =
        new DelegatingObjectInputFilter(api, "the-pattern", singleton("class-name"));

    Throwable thrown = catchThrowable(() -> {
      objectInputFilter.setFilterOn(objectInputStream);
    });

    assertThat(thrown)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasRootCause(exception);
  }

  @Test
  public void propagatesInvocationTargetExceptionInUnsupportedOperationException()
      throws InvocationTargetException, IllegalAccessException {
    InvocationTargetException exception =
        new InvocationTargetException(new Exception("testing"), "testing");
    doThrow(exception).when(api).setObjectInputFilter(same(objectInputStream), any());
    ObjectInputFilter objectInputFilter =
        new DelegatingObjectInputFilter(api, "the-pattern", singleton("class-name"));

    Throwable thrown = catchThrowable(() -> {
      objectInputFilter.setFilterOn(objectInputStream);
    });

    assertThat(thrown)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasCause(exception);
  }
}
