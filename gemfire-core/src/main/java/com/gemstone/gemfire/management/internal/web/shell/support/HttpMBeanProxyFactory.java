/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.shell.support;

import java.lang.reflect.Proxy;
import javax.management.ObjectName;

import com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker;

/**
 * The HttpMBeanProxyFactory class is an abstract factory for creating
 * <p/>
 * @author John Blum
 * @see java.lang.reflect.Proxy
 * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker
 * @since 8.0
 */
@SuppressWarnings("unused")
public class HttpMBeanProxyFactory {

  @SuppressWarnings("unchecked")
  public static <T> T createMBeanProxy(final HttpOperationInvoker connection, final ObjectName objectName, final Class<T> mbeanInterface) {
    return mbeanInterface.cast(Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
      new Class<?>[] { mbeanInterface }, new HttpInvocationHandler(connection, objectName)));
  }

}
