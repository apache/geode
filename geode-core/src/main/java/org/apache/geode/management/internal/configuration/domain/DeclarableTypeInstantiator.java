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
package org.apache.geode.management.internal.configuration.domain;

import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.configuration.ClassNameType;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.ObjectType;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.internal.services.classloader.impl.ClassLoaderServiceInstance;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.util.internal.UncheckedUtils;

public abstract class DeclarableTypeInstantiator {

  public static <T extends Declarable> T newInstance(DeclarableType declarableType, Cache cache) {
    try {
      ServiceResult<Class<?>> serviceResult =
          ClassLoaderServiceInstance.getInstance().forName(declarableType.getClassName());
      if (serviceResult.isSuccessful()) {
        T declarable = (UncheckedUtils.uncheckedCast(serviceResult.getMessage().newInstance()));
        Properties initProperties = new Properties();
        for (ParameterType parameter : declarableType.getParameters()) {
          initProperties.put(parameter.getName(), newInstance(parameter, cache));
        }
        declarable.initialize(cache, initProperties);
        return declarable;
      } else {
        throw new ClassNotFoundException(String.format("No class found for name: %s because %s",
            declarableType.getClassName(), serviceResult.getErrorMessage()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Error instantiating class: <" + declarableType.getClassName() + ">", e);
    }
  }


  public static <T> T newInstance(ObjectType objectType, Cache cache) {
    if (objectType.getString() != null) {
      return (T) objectType.getString();
    }

    if (objectType.getDeclarable() != null) {
      return newInstance(objectType.getDeclarable(), cache);
    }

    return null;
  }

  public static <V> V newInstance(ClassName type, Cache cache) {
    try {
      ServiceResult<Class<?>> serviceResult =
          ClassLoaderServiceInstance.getInstance().forName(type.getClassName());
      if (serviceResult.isSuccessful()) {
        V object = UncheckedUtils.uncheckedCast(serviceResult.getMessage().newInstance());
        if (object instanceof Declarable) {
          Declarable declarable = (Declarable) object;
          declarable.initialize(cache, type.getInitProperties());
          declarable.init(type.getInitProperties()); // for backwards compatibility
        }
        return object;
      } else {
        throw new ClassNotFoundException(String.format("No class found for name: %s because %s",
            type.getClassName(), serviceResult.getErrorMessage()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating class: <" + type.getClassName() + ">", e);
    }
  }

  public static <V> V newInstance(ClassNameType className) {
    try {
      ServiceResult<Class<?>> serviceResult =
          ClassLoaderServiceInstance.getInstance().forName(className.getClassName());
      if (serviceResult.isSuccessful()) {
        return UncheckedUtils.uncheckedCast(serviceResult.getMessage().newInstance());
      } else {
        throw new ClassNotFoundException(String.format("No class found for name: %s because %s",
            className.getClassName(), serviceResult.getErrorMessage()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating class: <" + className.getClassName() + ">",
          e);
    }
  }
}
