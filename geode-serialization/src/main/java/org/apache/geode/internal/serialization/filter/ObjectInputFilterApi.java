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

import org.jetbrains.annotations.TestOnly;

/**
 * Adapter interface that defines a layer of indirection for JDK's {@code ObjectInputFilter} class.
 *
 * <p>
 * {@code ObjectInputFilter} is absent in builds of Java 8 prior to 121. It is present in later
 * builds of Java 8 as {@code sun.misc.ObjectInputFilter} and underwent changes in Java 9 including
 * being moved to {@code java.io.ObjectInputFilter}.
 */
interface ObjectInputFilterApi {

  /**
   * Get the filter for classes being deserialized on the ObjectInputStream.
   *
   * <p>
   * Invokes {@code getObjectInputFilter} on {@code ObjectInputFilter$Config}.
   */
  Object getObjectInputFilter(ObjectInputStream inputStream)
      throws InvocationTargetException, IllegalAccessException;

  /**
   * Set the process-wide filter if it has not already been configured or set.
   *
   * <p>
   * Invokes {@code setObjectInputFilter} on {@code ObjectInputFilter$Config}
   */
  void setObjectInputFilter(ObjectInputStream inputStream, Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException;

  /**
   * Returns the process-wide serialization filter or null if not configured.
   *
   * <p>
   * Invokes {@code getSerialFilter} on {@code ObjectInputFilter$Config}
   */
  Object getSerialFilter()
      throws InvocationTargetException, IllegalAccessException;

  /**
   * Set the process-wide filter if it has not already been configured or set.
   *
   * <p>
   * Invokes {@code setSerialFilter} on {@code ObjectInputFilter$Config}
   */
  void setSerialFilter(Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException;

  /**
   * Returns an ObjectInputFilter from a string of patterns. The syntax is defined in the javadocs
   * of the JREs {@code (sun.misc. or java.io.)ObjectInputFilter$Config}.
   *
   * <p>
   * Invokes {@code createFilter} on {@code ObjectInputFilter$Config}.
   */
  Object createFilter(String pattern)
      throws InvocationTargetException, IllegalAccessException;

  /**
   * Returns a Java Proxy with InvocationHandler to allow Geode to inject custom behavior before
   * invoking the methods on the JREs {@code (sun.misc. or java.io.)ObjectInputFilter$Config}.
   */
  Object createObjectInputFilterProxy(String pattern, Collection<String> sanctionedClasses)
      throws InvocationTargetException, IllegalAccessException;

  @TestOnly
  Class<?> getObjectInputFilterClass();
}
