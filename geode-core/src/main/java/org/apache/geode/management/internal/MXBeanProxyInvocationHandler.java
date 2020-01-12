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

import static java.util.Arrays.asList;

import java.io.InvalidObjectException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.openmbean.OpenDataException;

import org.apache.geode.annotations.Immutable;

/**
 * This proxy handler handles all the method call invoked on an MXBean It follows same route as
 * MBeanProxyInvocationHandler Only difference is after obtaining the result it transforms the open
 * type to the actual java type
 */
public class MXBeanProxyInvocationHandler {

  @Immutable
  private static final Comparator<Method> METHOD_ORDER_COMPARATOR = new MethodOrder();

  private final ObjectName objectName;
  private final MBeanProxyInvocationHandler proxyHandler;
  private final Map<Method, MethodHandler> methodHandlerMap;

  MXBeanProxyInvocationHandler(ObjectName objectName, Class<?> mxbeanInterface,
      MBeanProxyInvocationHandler proxyHandler) throws IllegalArgumentException {
    this(objectName, mxbeanInterface, proxyHandler, OpenTypeUtil.newMap());
  }

  private MXBeanProxyInvocationHandler(ObjectName objectName, Class<?> mxbeanInterface,
      MBeanProxyInvocationHandler proxyHandler, Map<Method, MethodHandler> methodHandlerMap)
      throws IllegalArgumentException {
    if (mxbeanInterface == null) {
      throw new IllegalArgumentException("mxbeanInterface must not be null");
    }

    this.objectName = objectName;
    this.proxyHandler = proxyHandler;
    this.methodHandlerMap = methodHandlerMap;

    initHandlers(mxbeanInterface);
  }

  public Object invoke(Object proxy, Method method, Object[] arguments)
      throws InvalidObjectException, OpenDataException, MBeanException {
    MethodHandler handler = methodHandlerMap.get(method);
    OpenMethod convertingMethod = handler.getConvertingMethod();

    Object[] openArgs = convertingMethod.toOpenParameters(arguments);
    Object result = handler.invoke(proxy, method, openArgs);
    return convertingMethod.fromOpenReturnValue(result);
  }

  private void initHandlers(Class<?> mxbeanInterface) {
    Method[] methodArray = mxbeanInterface.getMethods();
    List<Method> methods = eliminateCovariantMethods(methodArray);

    for (Method method : methods) {
      String name = method.getName();

      String attributeName = "";
      if (name.startsWith("get")) {
        attributeName = name.substring(3);
      } else if (name.startsWith("is") && method.getReturnType() == boolean.class) {
        attributeName = name.substring(2);
      }

      if (!attributeName.isEmpty() && method.getParameterTypes().length == 0
          && method.getReturnType() != void.class) {
        // For Getters
        methodHandlerMap.put(method, new GetterHandler(attributeName, OpenMethod.from(method)));
      } else if (name.startsWith("set") && name.length() > 3
          && method.getParameterTypes().length == 1 && method.getReturnType() == void.class) {
        // For Setters
        methodHandlerMap.put(method, new SetterHandler(attributeName, OpenMethod.from(method)));
      } else {
        methodHandlerMap.put(method, new OpHandler(attributeName, OpenMethod.from(method)));
      }
    }
  }

  /**
   * Eliminate methods that are overridden with a covariant return type. Reflection will return both
   * the original and the overriding method but we need only the overriding one is of interest
   *
   * @return the method after eliminating covariant methods
   */
  private static List<Method> eliminateCovariantMethods(Method[] methodArray) {
    int methodCount = methodArray.length;
    Method[] sorted = methodArray.clone();
    Arrays.sort(sorted, METHOD_ORDER_COMPARATOR);
    Set<Method> overridden = OpenTypeUtil.newSet();

    for (int i = 1; i < methodCount; i++) {
      Method m0 = sorted[i - 1];
      Method m1 = sorted[i];

      if (!m0.getName().equals(m1.getName())) {
        continue;
      }

      if (Arrays.equals(m0.getParameterTypes(), m1.getParameterTypes())) {
        overridden.add(m0);
      }
    }

    List<Method> methods = OpenTypeUtil.newList(asList(methodArray));
    methods.removeAll(overridden);
    return methods;
  }

  /**
   * Handler for MXBean Proxy
   */
  private abstract static class MethodHandler {

    private final String name;
    private final OpenMethod convertingMethod;

    MethodHandler(String name, OpenMethod convertingMethod) {
      this.name = name;
      this.convertingMethod = convertingMethod;
    }

    String getName() {
      return name;
    }

    abstract Object invoke(Object proxy, Method method, Object[] arguments) throws MBeanException;

    private OpenMethod getConvertingMethod() {
      return convertingMethod;
    }
  }

  private class GetterHandler extends MethodHandler {

    private GetterHandler(String attributeName, OpenMethod convertingMethod) {
      super(attributeName, convertingMethod);
    }

    @Override
    Object invoke(Object proxy, Method method, Object[] arguments) throws MBeanException {
      String methodName = method.getName();
      String attributeName = "";
      if (methodName.startsWith("get")) {
        attributeName = methodName.substring(3);
      } else if (methodName.startsWith("is") && method.getReturnType() == boolean.class) {
        attributeName = methodName.substring(2);
      }
      return proxyHandler.delegateToObjectState(attributeName);
    }
  }

  private class SetterHandler extends MethodHandler {

    private SetterHandler(String attributeName, OpenMethod convertingMethod) {
      super(attributeName, convertingMethod);
    }

    @Override
    Object invoke(Object proxy, Method method, Object[] arguments) throws MBeanException {
      String methodName = method.getName();
      Class[] parameterTypes = method.getParameterTypes();
      String[] signature = new String[parameterTypes.length];

      for (int i = 0; i < parameterTypes.length; i++) {
        signature[i] = parameterTypes[i].getName();
      }

      return proxyHandler.delegateToFunctionService(objectName, methodName, arguments, signature);
    }
  }

  private class OpHandler extends MethodHandler {

    private OpHandler(String operationName, OpenMethod convertingMethod) {
      super(operationName, convertingMethod);
    }

    @Override
    Object invoke(Object proxy, Method method, Object[] arguments) throws MBeanException {
      String methodName = method.getName();
      Class[] parameterTypes = method.getParameterTypes();
      String[] signature = new String[parameterTypes.length];

      for (int i = 0; i < parameterTypes.length; i++) {
        signature[i] = parameterTypes[i].getName();
      }

      return proxyHandler.delegateToFunctionService(objectName, methodName, arguments, signature);
    }
  }

  /**
   * A comparator that defines a total order so that methods have the same name and identical
   * signatures appear next to each others. The methods are sorted in such a way that methods which
   * override each other will sit next to each other, with the overridden method first - e.g. Object
   * getFoo() is placed before Integer getFoo(). This makes it possible to determine whether a
   * method overrides another one simply by looking at the method(s) that precedes it in the list.
   * (see eliminateCovariantMethods).
   **/
  private static class MethodOrder implements Comparator<Method> {

    @Override
    public int compare(Method a, Method b) {
      int cmp = a.getName().compareTo(b.getName());
      if (cmp != 0) {
        return cmp;
      }
      Class<?>[] aparams = a.getParameterTypes();
      Class<?>[] bparams = b.getParameterTypes();
      if (aparams.length != bparams.length) {
        return aparams.length - bparams.length;
      }
      if (!Arrays.equals(aparams, bparams)) {
        return Arrays.toString(aparams).compareTo(Arrays.toString(bparams));
      }
      Class<?> aret = a.getReturnType();
      Class<?> bret = b.getReturnType();
      if (aret == bret) {
        return 0;
      }

      if (aret.isAssignableFrom(bret)) {
        return -1;
      }
      return +1;
    }
  }
}
