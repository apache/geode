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
import java.util.Collection;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * ObjectInputFilterApi isolates Geode from the JDK's ObjectInputFilter class, which is absent in
 * builds of Java 8 prior to 121 and is sun.misc.ObjectInputFilter in later builds but is
 * java.io.ObjectInputFilter in Java 9.
 */
@VisibleForTesting
public interface ObjectInputFilterApi {

  Object getObjectInputFilter(ObjectInputStream inputStream)
      throws InvocationTargetException, IllegalAccessException;

  void setObjectInputFilter(ObjectInputStream inputStream, Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException;

  Object getSerialFilter()
      throws InvocationTargetException, IllegalAccessException;

  void setSerialFilter(Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException;

  Object createFilter(String pattern) throws InvocationTargetException, IllegalAccessException;

  Object createObjectInputFilterProxy(String pattern, Collection<String> sanctionedClasses)
      throws InvocationTargetException, IllegalAccessException;
}
