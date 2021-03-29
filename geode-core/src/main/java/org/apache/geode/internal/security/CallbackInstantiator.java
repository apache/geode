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
package org.apache.geode.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.internal.ClassLoadUtils.classFromName;
import static org.apache.geode.internal.ClassLoadUtils.methodFromName;

import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;

/**
 * Utility methods for instantiating security callback objects by reflection.
 */
public class CallbackInstantiator {

  /**
   * this method would never return null, it either throws an exception or returns an object
   */
  public static <T> T getObjectOfTypeFromClassName(String className, Class<T> expectedClazz) {
    Class actualClass;
    try {
      actualClass = classFromName(className);
    } catch (Exception e) {
      throw new GemFireSecurityException("Instance could not be obtained, " + e, e);
    }

    if (!expectedClazz.isAssignableFrom(actualClass)) {
      throw new GemFireSecurityException(
          "Instance could not be obtained. Expecting a " + expectedClazz.getName() + " class.");
    }

    try {
      return (T) actualClass.newInstance();
    } catch (Exception e) {
      throw new GemFireSecurityException(
          "Instance could not be obtained. Error instantiating " + actualClass.getName(), e);
    }
  }

  public static SecurityManager getSecurityManager(Properties properties) {
    String securityManagerConfig = properties.getProperty(SECURITY_MANAGER);
    if (StringUtils.isBlank(securityManagerConfig)) {
      return null;
    }
    SecurityManager securityManager =
        getObjectOfTypeFromClassName(securityManagerConfig, SecurityManager.class);
    securityManager.init(properties);
    return securityManager;
  }

  public static PostProcessor getPostProcessor(Properties properties) {
    String postProcessorConfig = properties.getProperty(SECURITY_POST_PROCESSOR);
    if (StringUtils.isBlank(postProcessorConfig)) {
      return null;
    }
    PostProcessor postProcessor =
        getObjectOfTypeFromClassName(postProcessorConfig, PostProcessor.class);
    postProcessor.init(properties);
    return postProcessor;
  }

  /**
   * this method would never return null, it either throws an exception or returns an object
   */
  private static <T> T getObjectOfTypeFromFactoryMethod(String factoryMethodName) {
    T actualObject;
    try {
      Method factoryMethod = methodFromName(factoryMethodName);
      actualObject = (T) factoryMethod.invoke(null, (Object[]) null);
    } catch (Exception e) {
      throw new GemFireSecurityException("Instance could not be obtained from " + factoryMethodName,
          e);
    }

    if (actualObject == null) {
      throw new GemFireSecurityException(
          "Instance could not be obtained from " + factoryMethodName);
    }

    return actualObject;
  }

  /**
   * this method would never return null, it either throws an exception or returns an object
   *
   * @return an object of type expectedClazz. This method would never return null. It either returns
   *         an non-null object or throws exception.
   */
  public static <T> T getObjectOfType(String classOrMethod, Class<T> expectedClazz) {
    T object;
    try {
      object = getObjectOfTypeFromClassName(classOrMethod, expectedClazz);
    } catch (Exception ignore) {
      object = getObjectOfTypeFromFactoryMethod(classOrMethod);
    }
    return object;
  }
}
