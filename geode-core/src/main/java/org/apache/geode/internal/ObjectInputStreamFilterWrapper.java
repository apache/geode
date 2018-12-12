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
package org.apache.geode.internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.distributed.internal.DistributedSystemService;
import org.apache.geode.internal.logging.LogService;


/**
 * ObjectInputStreamFilterWrapper isolates InternalDataSerializer from the JDK's
 * ObjectInputFilter class, which is absent in builds of java8 prior to 121 and
 * is sun.misc.ObjectInputFilter in later builds but is java.io.ObjectInputFilter
 * in java9.
 * <p>
 * This class uses reflection and dynamic proxies to find the class and create a
 * serialization filter. Once java8 is retired and no longer supported by Geode the
 * use of reflection should be removed ObjectInputFilter can be directly used by
 * InternalDataSerializer.
 */
public class ObjectInputStreamFilterWrapper implements InputStreamFilter {
  private static final Logger logger = LogService.getLogger();

  private boolean usingJava8;
  private Object serializationFilter;

  private Class configClass; // ObjectInputFilter$Config
  private Method setObjectInputFilterMethod; // method on ObjectInputFilter$Config or
                                             // ObjectInputStream
  private Method createFilterMethod; // method on ObjectInputFilter$Config

  private Class filterClass; // ObjectInputFilter
  private Object ALLOWED; // field on ObjectInputFilter
  private Object REJECTED; // field on ObjectInputFilter
  private Method checkInputMethod; // method on ObjectInputFilter

  private Class filterInfoClass; // ObjectInputFilter$FilterInfo
  private Method serialClassMethod; // method on ObjectInputFilter$FilterInfo

  public ObjectInputStreamFilterWrapper(String serializationFilterSpec,
      Collection<DistributedSystemService> services) {

    Set<String> sanctionedClasses = new HashSet<>(500);
    for (DistributedSystemService service : services) {
      try {
        Collection<String> classNames = service.getSerializationAcceptlist();
        logger.info("loaded {} sanctioned serializables from {}", classNames.size(),
            service.getClass().getSimpleName());
        sanctionedClasses.addAll(classNames);
      } catch (IOException e) {
        throw new InternalGemFireException("error initializing serialization filter for " + service,
            e);
      }
    }

    try {
      URL sanctionedSerializables = ClassPathLoader.getLatest()
          .getResource(InternalDataSerializer.class, "sanctioned-geode-core-serializables.txt");
      Collection<String> coreClassNames =
          InternalDataSerializer.loadClassNames(sanctionedSerializables);
      sanctionedClasses.addAll(coreClassNames);
    } catch (IOException e) {
      throw new InternalGemFireException(
          "unable to read sanctionedSerializables.txt to form a serialization acceptlist", e);
    }

    logger.info("setting a serialization filter containing {}", serializationFilterSpec);

    // try java8 - this will not throw an exception if ObjectInputFilter can't be found
    if (createJava8Filter(serializationFilterSpec, sanctionedClasses)) {
      return;
    }

    // try java9 - this throws an exception if it fails to create the filter
    createJava9Filter(serializationFilterSpec, sanctionedClasses);
  }

  @Override
  public void setFilterOn(ObjectInputStream objectInputStream) {
    try {
      if (usingJava8) {
        setObjectInputFilterMethod.invoke(configClass, objectInputStream, serializationFilter);
      } else {
        setObjectInputFilterMethod.invoke(objectInputStream, serializationFilter);
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new InternalGemFireError("Unable to filter serialization", e);
    }
  }

  /**
   * java8 has sun.misc.ObjectInputFilter and uses ObjectInputFilter$Config.setObjectInputFilter()
   */
  private boolean createJava8Filter(String serializationFilterSpec,
      Collection<String> sanctionedClasses) {
    ClassPathLoader classPathLoader = ClassPathLoader.getLatest();
    try {

      filterInfoClass = classPathLoader.forName("sun.misc.ObjectInputFilter$FilterInfo");
      serialClassMethod = filterInfoClass.getDeclaredMethod("serialClass");

      filterClass = classPathLoader.forName("sun.misc.ObjectInputFilter");
      checkInputMethod = filterClass.getDeclaredMethod("checkInput", filterInfoClass);

      Class statusClass = classPathLoader.forName("sun.misc.ObjectInputFilter$Status");
      ALLOWED = statusClass.getEnumConstants()[1];
      REJECTED = statusClass.getEnumConstants()[2];
      if (!ALLOWED.toString().equals("ALLOWED") || !REJECTED.toString().equals("REJECTED")) {
        throw new GemFireConfigException(
            "ObjectInputFilter$Status enumeration in this JDK is not as expected");
      }

      configClass = classPathLoader.forName("sun.misc.ObjectInputFilter$Config");
      setObjectInputFilterMethod = configClass.getDeclaredMethod("setObjectInputFilter",
          ObjectInputStream.class, filterClass);
      createFilterMethod = configClass.getDeclaredMethod("createFilter", String.class);

      serializationFilter = createSerializationFilter(serializationFilterSpec, sanctionedClasses);
      usingJava8 = true;

    } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException
        | NoSuchMethodException e) {
      if (filterInfoClass != null) {
        throw new GemFireConfigException(
            "A serialization filter has been specified but Geode was unable to configure a filter",
            e);
      }
      return false;
    }

    return true;
  }

  /** java9 has java.io.ObjectInputFilter and uses ObjectInputStream.setObjectInputFilter() */
  private void createJava9Filter(String serializationFilterSpec,
      Collection<String> sanctionedClasses) {
    try {
      ClassPathLoader classPathLoader = ClassPathLoader.getLatest();

      filterInfoClass = classPathLoader.forName("java.io.ObjectInputFilter$FilterInfo");
      serialClassMethod = filterInfoClass.getDeclaredMethod("serialClass");


      filterClass = classPathLoader.forName("java.io.ObjectInputFilter");
      checkInputMethod = filterClass.getDeclaredMethod("checkInput", filterInfoClass);

      Class statusClass = classPathLoader.forName("java.io.ObjectInputFilter$Status");
      ALLOWED = statusClass.getEnumConstants()[1];
      REJECTED = statusClass.getEnumConstants()[2];
      if (!ALLOWED.toString().equals("ALLOWED") || !REJECTED.toString().equals("REJECTED")) {
        throw new GemFireConfigException(
            "ObjectInputFilter$Status enumeration in this JDK is not as expected");
      }

      configClass = classPathLoader.forName("java.io.ObjectInputFilter$Config");
      setObjectInputFilterMethod =
          ObjectInputStream.class.getDeclaredMethod("setObjectInputFilter", filterClass);
      createFilterMethod = configClass.getDeclaredMethod("createFilter", String.class);

      serializationFilter = createSerializationFilter(serializationFilterSpec, sanctionedClasses);

    } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException
        | NoSuchMethodException e) {
      throw new GemFireConfigException(
          "A serialization filter has been specified but Geode was unable to configure a filter",
          e);
    }
  }


  private Object createSerializationFilter(String serializationFilterSpec,
      Collection<String> sanctionedClasses)
      throws InvocationTargetException, IllegalAccessException {

    /*
     * create a user filter with the serialization acceptlist/denylist. This will be wrapped
     * by a filter that accept-lists sanctioned classes
     */
    Object userFilter = createFilterMethod.invoke(null, serializationFilterSpec);

    InvocationHandler handler = (proxy, method, args) -> {
      switch (method.getName()) {
        case "checkInput":
          Object filterInfo = args[0];
          Class serialClass = (Class) serialClassMethod.invoke(filterInfo);
          if (serialClass == null) { // no class to check, so nothing to accept-list
            return checkInputMethod.invoke(userFilter, filterInfo);
          }
          String className = serialClass.getName();
          if (serialClass.isArray()) {
            className = serialClass.getComponentType().getName();
          }
          if (sanctionedClasses.contains(className)) {
            return ALLOWED;
          } else {
            Object status = checkInputMethod.invoke(userFilter, filterInfo);
            if (status == REJECTED) {
              logger.fatal("Serialization filter is rejecting class {}", className,
                  new Exception(""));
            }
            return status;
          }
        default:
          throw new UnsupportedOperationException(
              "ObjectInputFilter." + method.getName() + " is not implemented");
      }
    };

    ClassPathLoader classPathLoader = ClassPathLoader.getLatest();
    return Proxy.newProxyInstance(classPathLoader.asClassLoader(), new Class[] {filterClass},
        handler);

  }


}
