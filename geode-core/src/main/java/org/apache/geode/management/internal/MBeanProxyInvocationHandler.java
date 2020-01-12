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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.openmbean.OpenDataException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class is the proxy handler for all the proxies created for federated MBeans. Its designed
 * with Java proxy mechanism. All data calls are delegated to the federation components. All method
 * calls are routed to specified members via Function service
 */
public class MBeanProxyInvocationHandler implements InvocationHandler {
  private static final Logger logger = LogService.getLogger();

  private final ObjectName objectName;
  private final Region<String, Object> monitoringRegion;
  private final DistributedMember member;
  private final NotificationBroadcasterSupport emitter;
  private final ProxyInterface proxyImpl;
  private final boolean isMXBean;

  private MXBeanProxyInvocationHandler mxBeanProxyInvocationHandler;

  static Object newProxyInstance(DistributedMember member, Region<String, Object> monitoringRegion,
      ObjectName objectName, FederationComponent federationComponent, Class interfaceClass) {
    boolean isMXBean = JMX.isMXBeanInterface(interfaceClass);
    boolean notificationBroadcaster = federationComponent.isNotificationEmitter();

    InvocationHandler invocationHandler =
        new MBeanProxyInvocationHandler(member, objectName, monitoringRegion, isMXBean);

    Class[] interfaces;
    if (notificationBroadcaster) {
      interfaces =
          new Class[] {interfaceClass, ProxyInterface.class, NotificationBroadCasterProxy.class};
    } else {
      interfaces = new Class[] {interfaceClass, ProxyInterface.class};
    }

    Object proxy = Proxy.newProxyInstance(MBeanProxyInvocationHandler.class.getClassLoader(),
        interfaces, invocationHandler);

    return interfaceClass.cast(proxy);
  }

  private MBeanProxyInvocationHandler(DistributedMember member, ObjectName objectName,
      Region<String, Object> monitoringRegion, boolean isMXBean) {
    this(member, objectName, monitoringRegion, isMXBean, new NotificationBroadcasterSupport(),
        new ProxyInterfaceImpl());
  }

  @VisibleForTesting
  MBeanProxyInvocationHandler(DistributedMember member, ObjectName objectName,
      Region<String, Object> monitoringRegion, boolean isMXBean,
      NotificationBroadcasterSupport emitter, ProxyInterface proxyImpl) {
    this.member = member;
    this.objectName = objectName;
    this.monitoringRegion = monitoringRegion;
    this.isMXBean = isMXBean;
    this.emitter = emitter;
    this.proxyImpl = proxyImpl;
  }

  /**
   * Inherited method from Invocation handler All object state requests are delegated to the
   * federated component.
   *
   * <p>
   * All setters and operations() are delegated to the function service.
   *
   * <p>
   * Notification emitter methods are also delegated to the function service
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] arguments)
      throws MBeanException, ListenerNotFoundException, InvalidObjectException, OpenDataException {
    if (logger.isTraceEnabled()) {
      logger.trace("Invoking Method {}", method.getName());
    }

    Class methodClass = method.getDeclaringClass();
    if (methodClass.equals(NotificationBroadcaster.class)
        || methodClass.equals(NotificationEmitter.class)) {
      return invokeBroadcasterMethod(method, arguments);
    }

    String methodName = method.getName();
    Class[] parameterTypes = method.getParameterTypes();
    Class returnType = method.getReturnType();

    int argumentCount = arguments == null ? 0 : arguments.length;

    if (methodName.equals("setLastRefreshedTime")) {
      proxyImpl.setLastRefreshedTime((Long) arguments[0]);
      return null;
    }
    if (methodName.equals("getLastRefreshedTime")) {
      return proxyImpl.getLastRefreshedTime();
    }
    if (methodName.equals("sendNotification")) {
      sendNotification(arguments[0]);
      return null;
    }

    // local or not: equals, toString, hashCode
    if (shouldDoLocally(method)) {
      return doLocally(method, arguments);
    }

    // Support For MXBean open types
    if (isMXBean) {
      MXBeanProxyInvocationHandler p = findMXBeanProxy(objectName, methodClass, this);
      return p.invoke(proxy, method, arguments);
    }

    if (methodName.startsWith("get") && methodName.length() > 3 && argumentCount == 0
        && !returnType.equals(Void.TYPE)) {
      return delegateToObjectState(methodName.substring(3));
    }

    if (methodName.startsWith("is") && methodName.length() > 2 && argumentCount == 0
        && (returnType.equals(Boolean.TYPE) || returnType.equals(Boolean.class))) {
      return delegateToObjectState(methodName.substring(2));
    }

    String[] signature = new String[parameterTypes.length];
    for (int i = 0; i < parameterTypes.length; i++) {
      signature[i] = parameterTypes[i].getName();
    }

    return delegateToFunctionService(objectName, methodName, arguments, signature);
  }

  /**
   * This will get the data from Object state which is replicated across the hidden region
   * FederationComponent being the carrier.
   */
  Object delegateToObjectState(String attributeName) throws MBeanException {
    try {
      FederationComponent federation =
          (FederationComponent) monitoringRegion.get(objectName.toString());
      return federation.getValue(attributeName);
    } catch (Exception e) {
      throw new MBeanException(e);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      throw new MBeanException(new Exception(t.getLocalizedMessage()));
    }
  }

  /**
   * It will call the Generic function to execute the method on the remote VM
   */
  Object delegateToFunctionService(ObjectName objectName, String methodName, Object[] arguments,
      String[] signature) throws MBeanException {
    Object[] functionArguments = new Object[5];
    functionArguments[0] = objectName;
    functionArguments[1] = methodName;
    functionArguments[2] = signature;
    functionArguments[3] = arguments;
    functionArguments[4] = member.getName();

    List<Object> result;
    try {
      ResultCollector resultCollector = FunctionService.onMember(member)
          .setArguments(functionArguments)
          .execute(ManagementConstants.MGMT_FUNCTION_ID);
      result = (List<Object>) resultCollector.getResult();
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(" Exception while Executing Function {}", e.getMessage(), e);
      }
      // Only in case of Exception caused for Function framework.
      return null;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      if (logger.isDebugEnabled()) {
        logger.debug(" Error while Executing Function {}", t.getMessage(), t);
      }
      return null;
    }

    return checkErrors(result.get(ManagementConstants.RESULT_INDEX));
  }

  /**
   * As this proxy may behave as an notification emitter it delegates to the member
   * NotificationBroadcasterSupport object
   */
  private void sendNotification(Object notification) {
    emitter.sendNotification((Notification) notification);
  }

  private Object checkErrors(Object lastResult) throws MBeanException {
    if (lastResult instanceof MBeanException) {
      // Convert all MBean public API exceptions to MBeanException
      throw (MBeanException) lastResult;
    }
    if (lastResult instanceof Exception) {
      return null;
    }
    if (lastResult instanceof Throwable) {
      return null;
    }
    return lastResult;
  }

  /**
   * The call will delegate to Managed Node for NotificationHub to register a local listener to
   * listen for notification from the MBean
   *
   * <p>
   * Moreover it will also add the client to local listener list by adding to the contained emitter.
   */
  private Object invokeBroadcasterMethod(Method method, Object[] arguments)
      throws MBeanException, ListenerNotFoundException {
    String methodName = method.getName();
    int argumentCount = arguments == null ? 0 : arguments.length;

    Class[] parameterTypes = method.getParameterTypes();
    String[] signature = new String[parameterTypes.length];

    switch (methodName) {
      case "addNotificationListener": {
        // The various throws of IllegalArgumentException here should not happen, since we know what
        // the methods in NotificationBroadcaster and NotificationEmitter are.

        if (argumentCount != 3) {
          throw new IllegalArgumentException(
              "Bad arg count to addNotificationListener: " + argumentCount);
        }

        // Other inconsistencies will produce ClassCastException below.

        NotificationListener listener = (NotificationListener) arguments[0];
        NotificationFilter filter = (NotificationFilter) arguments[1];
        Object handback = arguments[2];
        emitter.addNotificationListener(listener, filter, handback);
        delegateToFunctionService(objectName, methodName, null, signature);
        return null;
      }

      case "removeNotificationListener": {
        // NullPointerException if method with no args, but that shouldn't happen because
        // removeNotificationListener does have args.
        NotificationListener listener = (NotificationListener) arguments[0];

        switch (argumentCount) {
          case 1:
            emitter.removeNotificationListener(listener);

            // No need to send listener and filter details to other members. We only need to send a
            // message saying remove the listener registered for this object on your side.

            delegateToFunctionService(objectName, methodName, null, signature);
            return null;

          case 3:
            NotificationFilter filter = (NotificationFilter) arguments[1];
            Object handback = arguments[2];
            emitter.removeNotificationListener(listener, filter, handback);

            delegateToFunctionService(objectName, methodName, null, signature);
            return null;

          default:
            throw new IllegalArgumentException(
                "Bad arg count to removeNotificationListener: " + argumentCount);
        }
      }

      case "getNotificationInfo":
        if (arguments != null) {
          throw new IllegalArgumentException("getNotificationInfo has " + "args");
        }

        if (!MBeanJMXAdapter.mbeanServer.isRegistered(objectName)) {
          return new MBeanNotificationInfo[0];
        }

        // MBean info is delegated to function service as intention is to get the info of the actual
        // mbean rather than the proxy

        Object obj = delegateToFunctionService(objectName, methodName, arguments, signature);
        if (obj instanceof String) {
          return new MBeanNotificationInfo[0];
        }
        MBeanInfo info = (MBeanInfo) obj;
        return info.getNotifications();

      default:
        throw new IllegalArgumentException("Bad method name: " + methodName);
    }
  }

  private boolean shouldDoLocally(Method method) {
    String methodName = method.getName();
    if ((methodName.equals("hashCode") || methodName.equals("toString"))
        && method.getParameterTypes().length == 0) {
      return true;
    }
    return methodName.equals("equals")
        && Arrays.equals(method.getParameterTypes(), new Class[] {Object.class});
  }

  private Object doLocally(Method method, Object[] arguments) {
    String methodName = method.getName();
    FederationComponent federation =
        (FederationComponent) monitoringRegion.get(objectName.toString());

    switch (methodName) {
      case "equals":
        return federation.equals(arguments[0]);
      case "toString":
        return federation.toString();
      case "hashCode":
        return federation.hashCode();
    }

    throw new IllegalArgumentException("Unexpected method name: " + methodName);
  }

  private MXBeanProxyInvocationHandler findMXBeanProxy(ObjectName objectName,
      Class<?> mbeanInterface, MBeanProxyInvocationHandler handler) {
    if (mxBeanProxyInvocationHandler == null) {
      synchronized (this) {
        try {
          mxBeanProxyInvocationHandler =
              new MXBeanProxyInvocationHandler(objectName, mbeanInterface, handler);
        } catch (IllegalArgumentException e) {
          String message =
              "Cannot make MXBean proxy for " + mbeanInterface.getName() + ": " + e.getMessage();
          throw new IllegalArgumentException(message, e.getCause());
        }
      }
    }
    return mxBeanProxyInvocationHandler;
  }

  /**
   * Internal implementation of all the generic proxy methods
   */
  private static class ProxyInterfaceImpl implements ProxyInterface {

    private volatile long lastRefreshedTime;

    private ProxyInterfaceImpl() {
      lastRefreshedTime = System.currentTimeMillis();
    }

    @Override
    public long getLastRefreshedTime() {
      return lastRefreshedTime;
    }

    @Override
    public void setLastRefreshedTime(long lastRefreshedTime) {
      this.lastRefreshedTime = lastRefreshedTime;
    }
  }
}
