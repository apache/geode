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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

/**
 * This proxy handler handles all the method call invoked on an MXBean It follows same route as
 * MBeanProxyInvocationHandler Only difference is after obtaining the result it transforms the open
 * type to the actual java type
 *
 *
 */
public class MXBeanProxyInvocationHandler {

  private ObjectName objectName;

  private MBeanProxyInvocationHandler proxyHandler;

  private final Map<Method, MethodHandler> methodHandlerMap = OpenTypeUtil.newMap();

  private LogWriter logger;

  public MXBeanProxyInvocationHandler(ObjectName objectName, Class<?> mxbeanInterface,
      MBeanProxyInvocationHandler proxyHandler) throws Exception {

    if (mxbeanInterface == null)
      throw new IllegalArgumentException("Null parameter");

    this.objectName = objectName;

    this.proxyHandler = proxyHandler;

    this.logger = InternalDistributedSystem.getLogger();
    this.initHandlers(mxbeanInterface);
  }

  // Introspect the mbeanInterface and initialize this object's maps.
  //
  private void initHandlers(Class<?> mbeanInterface) throws Exception {
    final Method[] methodArray = mbeanInterface.getMethods();

    final List<Method> methods = eliminateCovariantMethods(methodArray);

    for (Method m : methods) {
      String name = m.getName();

      String attrName = "";
      if (name.startsWith("get")) {
        attrName = name.substring(3);
      } else if (name.startsWith("is") && m.getReturnType() == boolean.class) {
        attrName = name.substring(2);
      }

      if (attrName.length() != 0 && m.getParameterTypes().length == 0
          && m.getReturnType() != void.class) { // For Getters

        methodHandlerMap.put(m, new GetterHandler(attrName, OpenMethod.from(m)));
      } else if (name.startsWith("set") && name.length() > 3 && m.getParameterTypes().length == 1
          && m.getReturnType() == void.class) { // For Setteres
        methodHandlerMap.put(m, new SetterHandler(attrName, OpenMethod.from(m)));
      } else {
        methodHandlerMap.put(m, new OpHandler(attrName, OpenMethod.from(m)));
      }
    }
  }

  /**
   * Eliminate methods that are overridden with a covariant return type. Reflection will return both
   * the original and the overriding method but we need only the overriding one is of interest
   *
   * @return the method after eliminating covariant menthods
   */
  static List<Method> eliminateCovariantMethods(Method[] methodArray) {

    final int len = methodArray.length;
    final Method[] sorted = methodArray.clone();
    Arrays.sort(sorted, MethodOrder.instance);
    final Set<Method> overridden = OpenTypeUtil.newSet();
    for (int i = 1; i < len; i++) {
      final Method m0 = sorted[i - 1];
      final Method m1 = sorted[i];

      if (!m0.getName().equals(m1.getName()))
        continue;

      if (Arrays.equals(m0.getParameterTypes(), m1.getParameterTypes())) {
        overridden.add(m0);
      }
    }

    final List<Method> methods = OpenTypeUtil.newList(Arrays.asList(methodArray));
    methods.removeAll(overridden);
    return methods;
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
    public int compare(Method a, Method b) {
      final int cmp = a.getName().compareTo(b.getName());
      if (cmp != 0)
        return cmp;
      final Class<?>[] aparams = a.getParameterTypes();
      final Class<?>[] bparams = b.getParameterTypes();
      if (aparams.length != bparams.length)
        return aparams.length - bparams.length;
      if (!Arrays.equals(aparams, bparams)) {
        return Arrays.toString(aparams).compareTo(Arrays.toString(bparams));
      }
      final Class<?> aret = a.getReturnType();
      final Class<?> bret = b.getReturnType();
      if (aret == bret)
        return 0;

      if (aret.isAssignableFrom(bret))
        return -1;
      return +1;
    }

    public static final MethodOrder instance = new MethodOrder();
  }

  /**
   * Hanlder for MXBean Proxy
   *
   *
   */
  private abstract class MethodHandler {
    MethodHandler(String name, OpenMethod cm) {
      this.name = name;
      this.convertingMethod = cm;
    }

    String getName() {
      return name;
    }

    OpenMethod getConvertingMethod() {
      return convertingMethod;
    }

    abstract Object invoke(Object proxy, Method method, Object[] args) throws Throwable;

    private final String name;
    private final OpenMethod convertingMethod;
  }

  private class GetterHandler extends MethodHandler {
    GetterHandler(String attributeName, OpenMethod cm) {
      super(attributeName, cm);
    }

    @Override
    Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      assert (args == null || args.length == 0);
      final String methodName = method.getName();
      String attrName = "";
      if (methodName.startsWith("get")) {
        attrName = methodName.substring(3);
      } else if (methodName.startsWith("is") && method.getReturnType() == boolean.class) {
        attrName = methodName.substring(2);

      }
      return proxyHandler.delegateToObjectState(attrName);
    }
  }

  private class SetterHandler extends MethodHandler {
    SetterHandler(String attributeName, OpenMethod cm) {
      super(attributeName, cm);
    }

    @Override
    Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      final String methodName = method.getName();
      final Class[] paramTypes = method.getParameterTypes();
      final String[] signature = new String[paramTypes.length];
      for (int i = 0; i < paramTypes.length; i++)
        signature[i] = paramTypes[i].getName();
      return proxyHandler.delegateToFucntionService(objectName, methodName, args, signature);

    }
  }

  private class OpHandler extends MethodHandler {

    OpHandler(String operationName, OpenMethod cm) {
      super(operationName, cm);

    }

    Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      final String methodName = method.getName();
      final Class[] paramTypes = method.getParameterTypes();
      final String[] signature = new String[paramTypes.length];
      for (int i = 0; i < paramTypes.length; i++)
        signature[i] = paramTypes[i].getName();
      return proxyHandler.delegateToFucntionService(objectName, methodName, args, signature);
    }

  }

  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    MethodHandler handler = methodHandlerMap.get(method);
    OpenMethod cm = handler.getConvertingMethod();

    Object[] openArgs = cm.toOpenParameters(args);
    Object result = handler.invoke(proxy, method, openArgs);
    return cm.fromOpenReturnValue(result);
  }

}
