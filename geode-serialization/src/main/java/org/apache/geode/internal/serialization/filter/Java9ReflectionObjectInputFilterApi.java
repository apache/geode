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

import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Java 9 or greater needs to override some API paths defined in
 * {@code ReflectionObjectInputFilterApi}.
 */
class Java9ReflectionObjectInputFilterApi extends ReflectionObjectInputFilterApi {

  private final Class<?> ObjectInputStream;
  private final Method ObjectInputStream_setObjectInputFilter;
  private final Method ObjectInputStream_getObjectInputFilter;

  Java9ReflectionObjectInputFilterApi(ApiPackage apiPackage)
      throws ClassNotFoundException, NoSuchMethodException {
    super(apiPackage);
    ObjectInputStream = ObjectInputStream();
    ObjectInputStream_setObjectInputFilter = ObjectInputStream_setObjectInputFilter();
    ObjectInputStream_getObjectInputFilter = ObjectInputStream_getObjectInputFilter();
  }

  private Class<?> ObjectInputStream() throws ClassNotFoundException {
    return Class.forName(apiPackage.qualify("ObjectInputStream"));
  }

  private Method ObjectInputStream_setObjectInputFilter() throws NoSuchMethodException {
    return ObjectInputStream.getDeclaredMethod("setObjectInputFilter", ObjectInputFilter);
  }

  private Method ObjectInputStream_getObjectInputFilter() throws NoSuchMethodException {
    return ObjectInputStream.getDeclaredMethod("getObjectInputFilter");
  }

  @Override
  public Object getObjectInputFilter(ObjectInputStream inputStream)
      throws InvocationTargetException, IllegalAccessException {
    return ObjectInputStream_getObjectInputFilter.invoke(inputStream);
  }

  @Override
  public void setObjectInputFilter(ObjectInputStream inputStream, Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException {
    ObjectInputStream_setObjectInputFilter.invoke(inputStream, objectInputFilter);
  }
}
