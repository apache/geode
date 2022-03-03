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
package org.apache.geode.management.internal;

import java.io.InvalidObjectException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import javax.management.MBeanException;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;

import org.apache.geode.annotations.Immutable;

/**
 * Each data type in corresponding to an Open type will have a
 *
 *
 */
public class OpenMethod {

  @Immutable
  private static final OpenType[] noOpenTypes = new OpenType[0];
  private static final String[] noStrings = new String[0];

  private final Method method;
  private final OpenTypeConverter returnTypeConverter;
  private final OpenTypeConverter[] paramTypeConverters;
  private final boolean paramConversionIsIdentity;

  /**
   * Static method to get the OpenMethod
   *
   * @return an open method
   */
  static OpenMethod from(Method m) {
    try {
      return new OpenMethod(m);
    } catch (OpenDataException ode) {
      final String msg = "Method " + m.getDeclaringClass().getName() + "." + m.getName()
          + " has parameter or return type that " + "cannot be translated into an open type";
      throw new IllegalArgumentException(msg, ode);
    }
  }

  Method getMethod() {
    return method;
  }

  Type getGenericReturnType() {
    return method.getGenericReturnType();
  }

  Type[] getGenericParameterTypes() {
    return method.getGenericParameterTypes();
  }

  String getName() {
    return method.getName();
  }

  OpenType getOpenReturnType() {
    return returnTypeConverter.getOpenType();
  }

  OpenType[] getOpenParameterTypes() {
    final OpenType[] types = new OpenType[paramTypeConverters.length];
    for (int i = 0; i < paramTypeConverters.length; i++) {
      types[i] = paramTypeConverters[i].getOpenType();
    }
    return types;
  }

  void checkCallFromOpen() throws IllegalArgumentException {
    try {
      for (OpenTypeConverter paramConverter : paramTypeConverters) {
        paramConverter.checkReconstructible();
      }
    } catch (InvalidObjectException e) {
      throw new IllegalArgumentException(e);
    }
  }

  void checkCallToOpen() throws IllegalArgumentException {
    try {
      returnTypeConverter.checkReconstructible();
    } catch (InvalidObjectException e) {
      throw new IllegalArgumentException(e);
    }
  }

  String[] getOpenSignature() {
    if (paramTypeConverters.length == 0) {
      return noStrings;
    }

    String[] sig = new String[paramTypeConverters.length];
    for (int i = 0; i < paramTypeConverters.length; i++) {
      sig[i] = paramTypeConverters[i].getOpenClass().getName();
    }
    return sig;
  }

  Object toOpenReturnValue(Object ret) throws OpenDataException {
    return returnTypeConverter.toOpenValue(ret);
  }

  Object fromOpenReturnValue(Object ret) throws InvalidObjectException {
    return returnTypeConverter.fromOpenValue(ret);
  }

  Object[] toOpenParameters(Object[] params) throws OpenDataException {
    if (paramConversionIsIdentity || params == null) {
      return params;
    }
    final Object[] oparams = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      oparams[i] = paramTypeConverters[i].toOpenValue(params[i]);
    }
    return oparams;
  }

  Object[] fromOpenParameters(Object[] params) throws InvalidObjectException {
    if (paramConversionIsIdentity || params == null) {
      return params;
    }
    final Object[] jparams = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      jparams[i] = paramTypeConverters[i].fromOpenValue(params[i]);
    }
    return jparams;
  }

  Object toOpenParameter(Object param, int paramNo) throws OpenDataException {
    return paramTypeConverters[paramNo].toOpenValue(param);
  }

  Object fromOpenParameter(Object param, int paramNo) throws InvalidObjectException {
    return paramTypeConverters[paramNo].fromOpenValue(param);
  }

  Object invokeWithOpenReturn(Object obj, Object[] params)
      throws MBeanException, IllegalAccessException, InvocationTargetException {
    final Object[] javaParams;
    try {
      javaParams = fromOpenParameters(params);
    } catch (InvalidObjectException e) {
      final String msg = methodName() + ": cannot convert parameters " + "from open values: " + e;
      throw new MBeanException(e, msg);
    }
    final Object javaReturn = method.invoke(obj, javaParams);
    try {
      return returnTypeConverter.toOpenValue(javaReturn);
    } catch (OpenDataException e) {
      final String msg = methodName() + ": cannot convert return " + "value to open value: " + e;
      throw new MBeanException(e, msg);
    }
  }

  private String methodName() {
    return method.getDeclaringClass() + "." + method.getName();
  }

  private OpenMethod(Method m) throws OpenDataException {
    method = m;
    returnTypeConverter = OpenTypeConverter.toConverter(m.getGenericReturnType());
    Type[] params = m.getGenericParameterTypes();
    paramTypeConverters = new OpenTypeConverter[params.length];
    boolean identity = true;
    for (int i = 0; i < params.length; i++) {
      paramTypeConverters[i] = OpenTypeConverter.toConverter(params[i]);
      identity &= paramTypeConverters[i].isIdentity();
    }
    paramConversionIsIdentity = identity;
  }

}
