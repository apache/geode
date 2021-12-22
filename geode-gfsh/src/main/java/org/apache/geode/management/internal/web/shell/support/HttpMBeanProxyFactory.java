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
package org.apache.geode.management.internal.web.shell.support;

import java.lang.reflect.Proxy;

import javax.management.ObjectName;

import org.apache.geode.management.internal.web.shell.HttpOperationInvoker;

/**
 * The HttpMBeanProxyFactory class is an abstract factory for creating
 * <p/>
 *
 * @see java.lang.reflect.Proxy
 * @see org.apache.geode.management.internal.web.shell.HttpOperationInvoker
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class HttpMBeanProxyFactory {

  public static <T> T createMBeanProxy(final HttpOperationInvoker connection,
      final ObjectName objectName, final Class<T> mbeanInterface) {
    return mbeanInterface
        .cast(Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
            new Class<?>[] {mbeanInterface}, new HttpInvocationHandler(connection, objectName)));
  }

}
