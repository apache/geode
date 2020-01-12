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
package org.apache.geode.test.dunit.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.SystemFailure;

/**
 * A class specialized for invoking methods via reflection.
 */
class MethodInvoker {

  /**
   * Invoke the method "methodName" on the class named "target". Return the result, including stack
   * trace (if any).
   */
  static MethodInvokerResult execute(String target, String methodName) {
    return execute(target, methodName, null);
  }

  /**
   * Executes the given static method on the given class with the given parameters.
   */
  static MethodInvokerResult execute(String target, String methodName, Object[] parameters) {
    try {
      // get the class
      Class targetClass = Class.forName(target);

      // invoke the method
      try {
        Class[] paramTypes;
        if (parameters == null) {
          paramTypes = new Class[0];

        } else {
          paramTypes = new Class[parameters.length];
          for (int i = 0; i < parameters.length; i++) {
            if (parameters[i] == null) {
              paramTypes[i] = null;

            } else {
              paramTypes[i] = parameters[i].getClass();
            }
          }
        }

        Method method = getMethod(targetClass, methodName, paramTypes);
        method.setAccessible(true);
        Object result = method.invoke(targetClass, parameters);
        return new MethodInvokerResult(result);

      } catch (InvocationTargetException e) {
        Throwable targetException = e.getTargetException();
        if (targetException == null) {
          return new MethodInvokerResult(null);

        } else {
          return new MethodInvokerResult(targetException);
        }
      }

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable t) {
      return new MethodInvokerResult(t);
    }
  }

  /**
   * Send the message "methodName" to the object "target". Return the result, including stack trace
   * (if any).
   */
  static MethodInvokerResult executeObject(Object target, String methodName) {
    return executeObject(target, methodName, null);
  }

  /**
   * Executes the given instance method on the given object with the given arguments.
   */
  static MethodInvokerResult executeObject(Object target, String methodName, Object[] arguments) {
    try {
      // get the class
      Class receiverClass = target.getClass();

      // invoke the method
      try {
        Class[] paramTypes;
        if (arguments == null) {
          paramTypes = new Class[0];

        } else {
          paramTypes = new Class[arguments.length];
          for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] == null) {
              paramTypes[i] = Object.class;

            } else {
              paramTypes[i] = arguments[i].getClass();
            }
          }
        }

        Method method = getMethod(receiverClass, methodName, paramTypes);
        method.setAccessible(true);
        Object result = method.invoke(target, arguments);
        return new MethodInvokerResult(result);

      } catch (InvocationTargetException e) {
        Throwable targetException = e.getTargetException();
        if (targetException == null) {
          return new MethodInvokerResult(null);

        } else {
          return new MethodInvokerResult(targetException);
        }
      }

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable t) {
      return new MethodInvokerResult(t);
    }
  }

  /**
   * Helper method that searches a class (and its superclasses) for a method with the given name and
   * parameter types.
   *
   * @throws NoSuchMethodException If the method cannot be found
   */
  private static Method getMethod(Class targetClass, String methodName, Class[] parameterTypes)
      throws NoSuchMethodException {
    List<Method> matchingMethods = new ArrayList<>();
    for (Class theClass = targetClass; theClass != null; theClass = theClass.getSuperclass()) {
      Method[] methods = theClass.getDeclaredMethods();

      NEXT_METHOD:

      for (Method method : methods) {
        if (!method.getName().equals(methodName)) {
          continue;
        }

        Class[] methodParameterTypes = method.getParameterTypes();
        if (methodParameterTypes.length != parameterTypes.length) {
          continue;
        }

        for (int j = 0; j < methodParameterTypes.length; j++) {
          if (parameterTypes[j] == null) {
            if (methodParameterTypes[j].isPrimitive()) {
              // this parameter is not ok, the parameter is a primitive and the value is null
              continue NEXT_METHOD;
            } else {
              // this parameter is ok, the argument is an object and the value is null
              continue;
            }
          }
          if (!methodParameterTypes[j].isAssignableFrom(parameterTypes[j])) {
            Class methodParameterType = methodParameterTypes[j];
            Class parameterType = parameterTypes[j];

            if (methodParameterType.isPrimitive()) {
              if (methodParameterType.equals(boolean.class) && parameterType.equals(Boolean.class)
                  || methodParameterType.equals(short.class) && parameterType.equals(Short.class)
                  || methodParameterType.equals(int.class) && parameterType.equals(Integer.class)
                  || methodParameterType.equals(long.class) && parameterType.equals(Long.class)
                  || methodParameterType.equals(float.class) && parameterType.equals(Float.class)
                  || methodParameterType.equals(double.class) && parameterType.equals(Double.class)
                  || methodParameterType.equals(char.class) && parameterType.equals(Character.class)
                  || methodParameterType.equals(byte.class) && parameterType.equals(Byte.class)) {

                // This parameter is okay, try the next one
                continue;
              }
            }
            continue NEXT_METHOD;
          }
        }

        matchingMethods.add(method);
      }

      // We want to check to make sure there aren't two
      // ambiguous methods on the same class. But a subclass
      // can still override a method on a super class, so we'll stop
      // if we found a method on the subclass.
      if (matchingMethods.size() > 0) {
        break;
      }
    }

    if (matchingMethods.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Could not find method ");
      sb.append(methodName);
      sb.append(" with ");
      sb.append(parameterTypes.length);
      sb.append(" parameters [");
      for (int i = 0; i < parameterTypes.length; i++) {
        String name = parameterTypes[i] == null ? null : parameterTypes[i].getName();
        sb.append(name);
        if (i < parameterTypes.length - 1) {
          sb.append(", ");
        }
      }
      sb.append("] in class ");
      sb.append(targetClass.getName());
      throw new NoSuchMethodException(sb.toString());
    }

    if (matchingMethods.size() > 1) {
      StringBuilder sb = new StringBuilder();
      sb.append("Method is ambiguous ");
      sb.append(methodName);
      sb.append(" with ");
      sb.append(parameterTypes.length);
      sb.append(" parameters [");
      for (int i = 0; i < parameterTypes.length; i++) {
        String name = parameterTypes[i] == null ? null : parameterTypes[i].getName();
        sb.append(name);
        if (i < parameterTypes.length - 1) {
          sb.append(", ");
        }
      }
      sb.append("] in class ");
      sb.append(targetClass.getName());
      sb.append(" methods=").append(matchingMethods);
      throw new NoSuchMethodException(sb.toString());

    } else {
      return matchingMethods.get(0);
    }
  }
}
