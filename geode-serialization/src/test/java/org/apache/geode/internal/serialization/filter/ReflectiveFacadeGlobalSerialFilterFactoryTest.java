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

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class ReflectiveFacadeGlobalSerialFilterFactoryTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void constructsDelegatingGlobalSerialFilter() {
    ObjectInputFilterApi api = mock(ObjectInputFilterApi.class);
    ReflectiveFacadeGlobalSerialFilterFactory factory =
        new ReflectiveFacadeGlobalSerialFilterFactory();

    GlobalSerialFilter filter = factory.create(api, "pattern", emptySet());

    assertThat(filter).isInstanceOf(ReflectiveFacadeGlobalSerialFilter.class);
  }

  @Test
  public void delegatesToObjectInputFilterApiToCreateObjectInputFilter()
      throws InvocationTargetException, IllegalAccessException {
    ObjectInputFilterApi api = mock(ObjectInputFilterApi.class);
    ReflectiveFacadeGlobalSerialFilterFactory factory =
        new ReflectiveFacadeGlobalSerialFilterFactory();
    GlobalSerialFilter filter = factory.create(api, "pattern", emptySet());
    Object objectInputFilter = mock(Object.class);

    when(api.createObjectInputFilterProxy(any(), any()))
        .thenReturn(objectInputFilter);

    filter.setFilter();

    verify(api).createObjectInputFilterProxy(any(), any());
  }

  @Test
  public void delegatesToObjectInputFilterApiToSetSerialFilter()
      throws InvocationTargetException, IllegalAccessException {
    ObjectInputFilterApi api = mock(ObjectInputFilterApi.class);
    ReflectiveFacadeGlobalSerialFilterFactory factory =
        new ReflectiveFacadeGlobalSerialFilterFactory();
    GlobalSerialFilter filter = factory.create(api, "pattern", emptySet());
    Object objectInputFilter = mock(Object.class);

    when(api.createObjectInputFilterProxy(any(), any()))
        .thenReturn(objectInputFilter);

    filter.setFilter();

    verify(api).setSerialFilter(objectInputFilter);
  }

  @Test
  public void throwsUnsupportedOperationExceptionCausedByClassNotFoundException_whenObjectInputFilterClassNotFound() {
    Supplier<ObjectInputFilterApi> objectInputFilterApiSupplier = () -> {
      throw new UnsupportedOperationException(
          "ObjectInputFilter is not available.",
          new ClassNotFoundException("sun.misc.ObjectInputFilter"));
    };

    Throwable thrown = catchThrowable(() -> {
      new ReflectiveFacadeGlobalSerialFilterFactory(objectInputFilterApiSupplier)
          .create("pattern", emptySet());
    });

    assertThat(thrown)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("ObjectInputFilter is not available.")
        .hasRootCauseInstanceOf(ClassNotFoundException.class)
        .hasRootCauseMessage("sun.misc.ObjectInputFilter");
  }
}
