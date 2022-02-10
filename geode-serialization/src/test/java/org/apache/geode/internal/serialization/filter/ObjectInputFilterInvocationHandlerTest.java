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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.junit.Before;
import org.junit.Test;

public class ObjectInputFilterInvocationHandlerTest {

  private static final String PATTERN = "*";

  private Class<?> ObjectInputFilter;
  private Method ObjectInputFilter_checkInput;
  private Method ObjectInputFilter_FilterInfo_serialClass;
  private Object ObjectInputFilter_Status_ALLOWED;
  private Object ObjectInputFilter_Status_REJECTED;

  private Object objectInputFilter;

  @Before
  public void setUp()
      throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    ReflectiveObjectInputFilterApi api =
        (ReflectiveObjectInputFilterApi) new ReflectiveObjectInputFilterApiFactory()
            .createObjectInputFilterApi();

    ObjectInputFilter = api.ObjectInputFilter;
    ObjectInputFilter_checkInput = api.ObjectInputFilter_checkInput;
    Class<?> objectInputFilter_Config = api.ObjectInputFilter_Config;
    Method objectInputFilter_Config_createFilter = api.ObjectInputFilter_Config_createFilter;
    ObjectInputFilter_Status_ALLOWED = api.ObjectInputFilter_Status_ALLOWED;
    ObjectInputFilter_Status_REJECTED = api.ObjectInputFilter_Status_REJECTED;
    ObjectInputFilter_FilterInfo_serialClass = api.ObjectInputFilter_FilterInfo_serialClass;

    objectInputFilter = objectInputFilter_Config_createFilter
        .invoke(objectInputFilter_Config, PATTERN);
  }

  @Test
  public void sanctionedClassesIsRequired() {
    Throwable thrown = catchThrowable(() -> {
      new ObjectInputFilterInvocationHandler(
          ObjectInputFilter_checkInput,
          ObjectInputFilter_FilterInfo_serialClass,
          ObjectInputFilter_Status_ALLOWED,
          ObjectInputFilter_Status_REJECTED,
          objectInputFilter,
          null);
    });

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void toStringIsInvokedOnProxiedInstance() {
    InvocationHandler invocationHandler = new ObjectInputFilterInvocationHandler(
        ObjectInputFilter_checkInput,
        ObjectInputFilter_FilterInfo_serialClass,
        ObjectInputFilter_Status_ALLOWED,
        ObjectInputFilter_Status_REJECTED,
        objectInputFilter,
        emptySet());
    Object proxy = objectInputFilterProxy(invocationHandler);

    String result = proxy.toString();

    assertThat(result).isEqualTo(PATTERN);
  }

  private Object objectInputFilterProxy(InvocationHandler invocationHandler) {
    return Proxy.newProxyInstance(
        getClass().getClassLoader(),
        new Class[] {ObjectInputFilter},
        invocationHandler);
  }
}
