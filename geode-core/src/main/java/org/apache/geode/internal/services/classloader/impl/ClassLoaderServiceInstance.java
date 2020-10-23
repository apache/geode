/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.services.classloader.impl;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.services.classloader.ClassLoaderService;

public class ClassLoaderServiceInstance {

  @Immutable
  private static ClassLoaderService classLoaderService;

  @Immutable
  private static Class<? extends ClassLoaderService> serviceClass =
      DefaultClassLoaderServiceImpl.class;

  public static synchronized ClassLoaderService getInstance() {
    if (classLoaderService == null) {
      classLoaderService = createClassLoaderService();
    }
    return classLoaderService;
  }

  private static ClassLoaderService createClassLoaderService() {
    try {
      return serviceClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
      throw new GemFireConfigException("Cannot instantiate service for class: " + serviceClass, e);
    }
  }

  public static synchronized void overrideServiceClass(Class<? extends ClassLoaderService> clazz) {
    if (classLoaderService == null) {
      serviceClass = clazz;
    } else {
      throw new GemFireConfigException(
          "Cannot override ClassLoaderService because it has already been created as type: "
              + classLoaderService.getClass());
    }
  }
}
