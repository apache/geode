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

import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * ReflectionObjectInputFilterApi uses reflection and dynamic proxies to find wrap the
 * ObjectInputFilter API. Once Java 8 is retired and no longer supported by Geode,
 * ObjectInputFilterApi should be removed and replaced by direct use of ObjectInputFilter.
 */
class ReflectionObjectInputFilterApi implements ObjectInputFilterApi {
  private static final Logger logger = LogService.getLogger();

  protected final ApiPackage apiPackage;

  // api.package.ObjectInputFilter
  protected final Class<?> ObjectInputFilter;
  private final Method ObjectInputFilter_checkInput;
  private final Object ObjectInputFilter_Status_ALLOWED;
  private final Object ObjectInputFilter_Status_REJECTED;

  // api.package.ObjectInputFilter$Config
  private final Class<?> ObjectInputFilter_Config;
  private final Method ObjectInputFilter_Config_createFilter;
  private final Method ObjectInputFilter_Config_getObjectInputFilter;
  private final Method ObjectInputFilter_Config_setObjectInputFilter;
  private final Method ObjectInputFilter_Config_getSerialFilter;
  private final Method ObjectInputFilter_Config_setSerialFilter;

  // api.package.ObjectInputFilter$FilterInfo
  private final Class<?> ObjectInputFilter_FilterInfo;
  private final Method ObjectInputFilter_FilterInfo_serialClass;

  /**
   * Use reflection to lookup the classes and methods for the API.
   */
  ReflectionObjectInputFilterApi(ApiPackage apiPackage)
      throws ClassNotFoundException, NoSuchMethodException {
    this.apiPackage = apiPackage;

    ObjectInputFilter = ObjectInputFilter();
    ObjectInputFilter_Config = ObjectInputFilter_Config();
    ObjectInputFilter_FilterInfo = ObjectInputFilter_FilterInfo();
    Class<?> ObjectInputFilter_Status = ObjectInputFilter_Status();
    ObjectInputFilter_Status_ALLOWED = ObjectInputFilter_Status.getEnumConstants()[1];
    ObjectInputFilter_Status_REJECTED = ObjectInputFilter_Status.getEnumConstants()[2];

    // Status enum includes UNDECIDED, ALLOWED, and REJECTED
    if (!ObjectInputFilter_Status_ALLOWED.toString().equals("ALLOWED")
        || !ObjectInputFilter_Status_REJECTED.toString().equals("REJECTED")) {
      throw new UnsupportedOperationException(
          "ObjectInputFilter$Status enumeration in this JDK is not as expected");
    }

    ObjectInputFilter_checkInput = ObjectInputFilter_checkInput();

    ObjectInputFilter_Config_createFilter = ObjectInputFilter_Config_createFilter();
    if (apiPackage == ApiPackage.SUN_MISC) {
      ObjectInputFilter_Config_getObjectInputFilter =
          ObjectInputFilter_Config_getObjectInputFilter();
      ObjectInputFilter_Config_setObjectInputFilter =
          ObjectInputFilter_Config_setObjectInputFilter();
    } else {
      ObjectInputFilter_Config_getObjectInputFilter = null;
      ObjectInputFilter_Config_setObjectInputFilter = null;
    }
    ObjectInputFilter_Config_getSerialFilter = ObjectInputFilter_Config_getSerialFilter();
    ObjectInputFilter_Config_setSerialFilter = ObjectInputFilter_Config_setSerialFilter();

    ObjectInputFilter_FilterInfo_serialClass = ObjectInputFilter_FilterInfo_serialClass();
  }

  @Override
  public Object getObjectInputFilter(ObjectInputStream inputStream)
      throws InvocationTargetException, IllegalAccessException {
    return ObjectInputFilter_Config_getObjectInputFilter.invoke(ObjectInputFilter_Config,
        inputStream);
  }

  @Override
  public void setObjectInputFilter(ObjectInputStream inputStream, Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException {
    ObjectInputFilter_Config_setObjectInputFilter.invoke(ObjectInputFilter_Config, inputStream,
        objectInputFilter);
  }

  @Override
  public Object getSerialFilter()
      throws InvocationTargetException, IllegalAccessException {
    return ObjectInputFilter_Config_getSerialFilter.invoke(ObjectInputFilter_Config);
  }

  @Override
  public void setSerialFilter(Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException {
    ObjectInputFilter_Config_setSerialFilter.invoke(ObjectInputFilter_Config, objectInputFilter);
  }

  @Override
  public Object createFilter(String pattern)
      throws InvocationTargetException, IllegalAccessException {
    return ObjectInputFilter_Config_createFilter.invoke(null, pattern);
  }

  @Override
  public Object createObjectInputFilterProxy(String pattern, Collection<String> sanctionedClasses)
      throws InvocationTargetException, IllegalAccessException {
    Object objectInputFilter =
        ObjectInputFilter_Config_createFilter.invoke(ObjectInputFilter_Config, pattern);

    /*
     * Members first connect to each other with sockets that are restricted to the Geode sanctioned
     * serializables which sets the Filter on that stream (an ObjectInputStream can only set a
     * Filter once in its lifetime). Geode then loads up its ConfigurationProperties and needs
     * to widen the Filter to include the User's sanctioned serializables if they provided any
     * (serializable-object-filter).
     *
     * Wrap the Filter in a dynamic Proxy to provide an InvocationHandler in order to decorate
     * before-calls to checkInput (checks the class against the filter). This allows us to
     * effectively override the call to widen the filter to include the User's addition to the set
     * of sanctioned serializables.
     */
    InvocationHandler invocationHandler = (proxy, method, args) -> {
      if (!"checkInput".equals(method.getName())) {
        throw new UnsupportedOperationException(
            "ObjectInputFilter." + method.getName() + " is not implemented");
      }

      // fetch the class of the serialized instance
      Object objectInputFilter_filterInfo = args[0];
      Class<?> serialClass =
          (Class<?>) ObjectInputFilter_FilterInfo_serialClass.invoke(objectInputFilter_filterInfo);
      System.out.println("JC debug: serialClass=" + serialClass);
      if (serialClass == null) { // no class to check, so nothing to accept-list
        return ObjectInputFilter_checkInput.invoke(objectInputFilter, objectInputFilter_filterInfo);
      }

      // check sanctionedClasses to determine if the name of the class is ALLOWED
      String serialClassName = serialClass.getName();
      System.out.println("JC debug: serialClassName" + serialClassName);
      if (serialClass.isArray()) {
        serialClassName = serialClass.getComponentType().getName();
      }
      if (sanctionedClasses.contains(serialClassName)) {
        return ObjectInputFilter_Status_ALLOWED;
      }

      // check the filter to determine if the class is ALLOWED
      Object objectInputFilter_Status =
          ObjectInputFilter_checkInput.invoke(objectInputFilter, objectInputFilter_filterInfo);
      if (objectInputFilter_Status == ObjectInputFilter_Status_REJECTED) {
        logger.fatal("Serialization filter is rejecting class {}", serialClassName,
            new InvalidClassException(serialClassName));
      }
      System.out.println("JC debug: objectInputFilter_Status" + objectInputFilter_Status);
      return objectInputFilter_Status;
    };

    // wrap the filter within a proxy to inject the above invocation handler
    return Proxy.newProxyInstance(
        ObjectInputFilter.getClassLoader(),
        new Class[] {ObjectInputFilter},
        invocationHandler);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ReflectionObjectInputFilterApi{");
    sb.append("apiPackage='").append(apiPackage).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public Class<?> getObjectInputFilterClass() {
    return ObjectInputFilter;
  }

  @VisibleForTesting
  ApiPackage getApiPackage() {
    return apiPackage;
  }

  /** {@code public interface ObjectInputFilter} */
  private Class<?> ObjectInputFilter() throws ClassNotFoundException {
    return Class.forName(apiPackage.qualify("ObjectInputFilter"));
  }

  /** {@code public Status checkInput(FilterInfo filterInfo)} */
  private Method ObjectInputFilter_checkInput()
      throws NoSuchMethodException {
    return ObjectInputFilter
        .getDeclaredMethod("checkInput", ObjectInputFilter_FilterInfo);
  }

  /** {@code public enum Status} */
  private Class<?> ObjectInputFilter_Status() throws ClassNotFoundException {
    return Class.forName(apiPackage.qualify("ObjectInputFilter$Status"));
  }

  /** {@code public final class Config} */
  private Class<?> ObjectInputFilter_Config() throws ClassNotFoundException {
    return Class.forName(apiPackage.qualify("ObjectInputFilter$Config"));
  }

  /** {@code public static ObjectInputFilter createFilter(String pattern)} */
  private Method ObjectInputFilter_Config_createFilter()
      throws NoSuchMethodException {
    return ObjectInputFilter_Config.getDeclaredMethod("createFilter", String.class);
  }

  /** {@code public static ObjectInputFilter getSerialFilter()} */
  private Method ObjectInputFilter_Config_getObjectInputFilter()
      throws NoSuchMethodException {
    return ObjectInputFilter_Config.getDeclaredMethod("getObjectInputFilter",
        ObjectInputStream.class);
  }

  /** {@code public static setSerialFilter(ObjectInputFilter filter)} */
  private Method ObjectInputFilter_Config_setObjectInputFilter()
      throws NoSuchMethodException {
    return ObjectInputFilter_Config.getDeclaredMethod("setObjectInputFilter",
        ObjectInputStream.class, ObjectInputFilter);
  }

  /** {@code public static ObjectInputFilter getSerialFilter()} */
  private Method ObjectInputFilter_Config_getSerialFilter()
      throws NoSuchMethodException {
    return ObjectInputFilter_Config.getDeclaredMethod("getSerialFilter");
  }

  /** {@code public static setSerialFilter(ObjectInputFilter filter)} */
  private Method ObjectInputFilter_Config_setSerialFilter()
      throws NoSuchMethodException {
    return ObjectInputFilter_Config.getDeclaredMethod("setSerialFilter", ObjectInputFilter);
  }

  /** {@code public interface FilterInfo} */
  private Class<?> ObjectInputFilter_FilterInfo() throws ClassNotFoundException {
    return Class.forName(apiPackage.qualify("ObjectInputFilter$FilterInfo"));
  }

  /** {@code public Class<?> serialClass()} */
  private Method ObjectInputFilter_FilterInfo_serialClass()
      throws NoSuchMethodException {
    return ObjectInputFilter_FilterInfo.getDeclaredMethod("serialClass");
  }
}
