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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;

import org.apache.geode.management.internal.web.shell.HttpOperationInvoker;

/**
 * The HttpInvocationHandler class is an implementation of InvocationHandler serving as a proxy that
 * uses HTTP remoting for the actual method invocation on the target resource.
 * <p/>
 *
 * @see java.lang.reflect.InvocationHandler
 * @see javax.management.ObjectName
 * @see org.apache.geode.management.internal.web.shell.HttpOperationInvoker
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class HttpInvocationHandler implements InvocationHandler {

  private final HttpOperationInvoker operationInvoker;

  private final ObjectName objectName;

  public HttpInvocationHandler(final HttpOperationInvoker operationInvoker,
      final ObjectName objectName) {
    assert operationInvoker != null : "The HTTP-based OperationInvoker cannot be null!";
    assert objectName != null : "The ObjectName of the resource on which the operation is invoked cannot be null!";
    this.operationInvoker = operationInvoker;
    this.objectName = objectName;
  }

  protected ObjectName getObjectName() {
    return objectName;
  }

  protected HttpOperationInvoker getOperationInvoker() {
    return operationInvoker;
  }

  protected boolean isAttributeAccessor(final Method method) {
    final String methodName = method.getName();
    return (methodName.startsWith("get") || methodName.startsWith("is"));
  }

  protected String getAttributeName(final Method method) {
    final String methodName = method.getName();
    // the attribute accessor method name either begins with 'get' or 'is' as defined by the JMX
    // specification
    final int beginIndex = (methodName.startsWith("get") ? 3 : 2);
    return methodName.substring(beginIndex);
  }

  protected String getResourceName() {
    return getObjectName().getCanonicalName();
  }

  protected String[] getSignature(final Method method) {
    final List<String> signature = new ArrayList<>();

    for (final Class<?> parameterType : method.getParameterTypes()) {
      signature.add(parameterType.getName());
    }

    if (signature.size() == 0) {
      return null;
    }

    return signature.toArray(new String[0]);
  }

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args)
      throws Throwable {
    if (isAttributeAccessor(method)) {
      return getOperationInvoker().getAttribute(getResourceName(), getAttributeName(method));
    } else {
      return getOperationInvoker().invoke(getResourceName(), method.getName(), args,
          getSignature(method));
    }
  }

}
