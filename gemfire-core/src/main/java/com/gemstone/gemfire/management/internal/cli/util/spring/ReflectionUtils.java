/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.util.spring;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Replaces org.springframework.shell.support.util.ReflectionUtils which
 * is now removed from SPring Shell & the same class is referred from Spring
 * Core. With this we can avoid GemFire member's runtime dependency on Spring
 * Core.
 */
/*
 * Code selectively taken from the original org.springframework.shell.support.util.ReflectionUtils
 */
public class ReflectionUtils {

  /**
   * Invoke the specified {@link Method} against the supplied target object
   * with the supplied arguments. The target object can be <code>null</code>
   * when invoking a static {@link Method}.
   * <p>Thrown exceptions are handled via a call to {@link #handleReflectionException}.
   * @param method the method to invoke
   * @param target the target object to invoke the method on
   * @param args the invocation arguments (may be <code>null</code>)
   * @return the invocation result, if any
   */
  public static Object invokeMethod(final Method method, final Object target, final Object[] args) {
    try {
      return method.invoke(target, args);
    }
    catch (Exception ex) {
      handleReflectionException(ex);
    }
    throw new IllegalStateException("Should never get here");
  }

  /**
   * Handle the given reflection exception. Should only be called if
   * no checked exception is expected to be thrown by the target method.
   * <p>Throws the underlying RuntimeException or Error in case of an
   * InvocationTargetException with such a root cause. Throws an
   * IllegalStateException with an appropriate message else.
   * @param ex the reflection exception to handle
   */
  public static void handleReflectionException(final Exception ex) {
    if (ex instanceof NoSuchMethodException) {
      throw new IllegalStateException("Method not found: " + ex.getMessage());
    }
    if (ex instanceof IllegalAccessException) {
      throw new IllegalStateException("Could not access method: " + ex.getMessage());
    }
    if (ex instanceof InvocationTargetException) {
      handleInvocationTargetException((InvocationTargetException) ex);
    }
    if (ex instanceof RuntimeException) {
      throw (RuntimeException) ex;
    }
    handleUnexpectedException(ex);
  }

  /**
   * Handle the given invocation target exception. Should only be called if
   * no checked exception is expected to be thrown by the target method.
   * <p>Throws the underlying RuntimeException or Error in case of such
   * a root cause. Throws an IllegalStateException else.
   * @param ex the invocation target exception to handle
   */
  public static void handleInvocationTargetException(final InvocationTargetException ex) {
    rethrowRuntimeException(ex.getTargetException());
  }

  /**
   * Rethrow the given {@link Throwable exception}, which is presumably the
   * <em>target exception</em> of an {@link InvocationTargetException}.
   * Should only be called if no checked exception is expected to be thrown by
   * the target method.
   * <p>Rethrows the underlying exception cast to an {@link RuntimeException}
   * or {@link Error} if appropriate; otherwise, throws an
   * {@link IllegalStateException}.
   * @param ex the exception to rethrow
   * @throws RuntimeException the rethrown exception
   */
  public static void rethrowRuntimeException(final Throwable ex) {
    if (ex instanceof RuntimeException) {
      throw (RuntimeException) ex;
    }
    if (ex instanceof Error) {
      throw (Error) ex;
    }
    handleUnexpectedException(ex);
  }

  /**
   * Throws an IllegalStateException with the given exception as root cause.
   * @param ex the unexpected exception
   */
  private static void handleUnexpectedException(final Throwable ex) {
    throw new IllegalStateException("Unexpected exception thrown", ex);
  }

}
