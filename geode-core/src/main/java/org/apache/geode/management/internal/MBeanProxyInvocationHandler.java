/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import javax.management.JMX;
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

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;




/**
 * This class is the proxy handler for all the proxies created for federated
 * MBeans. Its designed with Java proxy mechanism. All data calls are 
 * delegated to the federation components.
 * All method calls are routed to specified members via Function service
 * 
 * 
 */

public class MBeanProxyInvocationHandler implements InvocationHandler {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * Name of the MBean
   */
  private ObjectName objectName;

  /**
   * The monitoring region where this Object resides.
   */
  private Region<String, Object> monitoringRegion;

  /**
   * The member to which this proxy belongs
   */

  private DistributedMember member;


  /**
   * emitter is a helper class for sending notifications on behalf of the proxy
   */
  private final NotificationBroadcasterSupport emitter;

  private final ProxyInterface proxyImpl;
 
  private boolean isMXBean;
  
  private MXBeanProxyInvocationHandler mxbeanInvocationRef;
  
  

  /**
   * 
   * @param member
   *          member to which this MBean belongs
   * @param monitoringRegion
   *          corresponding MonitoringRegion
   * @param objectName
   *          ObjectName of the MBean
   * @param interfaceClass
   *          on which interface the proxy to be exposed
   * @return Object
   * @throws ClassNotFoundException
   * @throws IntrospectionException
   */
  public static Object newProxyInstance(DistributedMember member,
      Region<String, Object> monitoringRegion, ObjectName objectName,
      Class interfaceClass) throws ClassNotFoundException,
      IntrospectionException {
    boolean isMXBean = JMX.isMXBeanInterface(interfaceClass);
    boolean notificationBroadcaster = ((FederationComponent) monitoringRegion
        .get(objectName.toString())).isNotificationEmitter();

    InvocationHandler handler = new MBeanProxyInvocationHandler(member,
        objectName, monitoringRegion, isMXBean);

    Class[] interfaces;

    if (notificationBroadcaster) {
      interfaces = new Class[] { interfaceClass, ProxyInterface.class,
          NotificationBroadCasterProxy.class };
    } else {
      interfaces = new Class[] { interfaceClass, ProxyInterface.class };
    }

    Object proxy = Proxy.newProxyInstance(MBeanProxyInvocationHandler.class
        .getClassLoader(), interfaces, handler);

    return interfaceClass.cast(proxy);
  }

  /**
   * 
   * @param member
   *          member to which this MBean belongs
   * @param objectName
   *          ObjectName of the MBean
   * @param monitoringRegion
   *          corresponding MonitoringRegion
   * @throws IntrospectionException
   * @throws ClassNotFoundException
   */
  private MBeanProxyInvocationHandler(DistributedMember member,
      ObjectName objectName, Region<String, Object> monitoringRegion, boolean isMXBean)
      throws IntrospectionException, ClassNotFoundException {
    this.member = member;
    this.objectName = objectName;
    this.monitoringRegion = monitoringRegion;
    this.emitter = new NotificationBroadcasterSupport();
    this.proxyImpl = new ProxyInterfaceImpl();
    this.isMXBean = isMXBean;
    
  }

  /**
   * Inherited method from Invocation handler All object state requests are
   * delegated to the federated component.
   * 
   * All setters and operations() are delegated to the function service.
   * 
   * Notification emmitter methods are also delegated to the function service
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {

    if (logger.isTraceEnabled()) {
      logger.trace("Invoking Method {}", method.getName());
    }
    final Class methodClass = method.getDeclaringClass();
   
    

    if (methodClass.equals(NotificationBroadcaster.class)
        || methodClass.equals(NotificationEmitter.class))
      return invokeBroadcasterMethod(proxy, method, args);

    final String methodName = method.getName();
    final Class[] paramTypes = method.getParameterTypes();
    final Class returnType = method.getReturnType();

  
    
    final int nargs = (args == null) ? 0 : args.length;

    if (methodName.equals("setLastRefreshedTime")) {
      proxyImpl.setLastRefreshedTime((Long) args[0]);
      return null;
    }
    if (methodName.equals("getLastRefreshedTime")) {
      return proxyImpl.getLastRefreshedTime();
    }

    if (methodName.equals("sendNotification")) {
      sendNotification(args[0]);
      return null;
    }
    
    // local or not: equals, toString, hashCode
    if (shouldDoLocally(proxy, method)){
      return doLocally(proxy, method, args);
    }
      
    // Support For MXBean open types
    if (isMXBean) {
      MXBeanProxyInvocationHandler p = findMXBeanProxy(objectName, methodClass, this);
      return p.invoke( proxy, method, args);
    }

    if (methodName.startsWith("get") && methodName.length() > 3 && nargs == 0
        && !returnType.equals(Void.TYPE)) {
      return delegateToObjectState(methodName.substring(3));
    }

    if (methodName.startsWith("is")
        && methodName.length() > 2
        && nargs == 0
        && (returnType.equals(Boolean.TYPE) || returnType.equals(Boolean.class))) {
      return delegateToObjectState(methodName.substring(2));
    }

    final String[] signature = new String[paramTypes.length];
    for (int i = 0; i < paramTypes.length; i++)
      signature[i] = paramTypes[i].getName();

    if (methodName.startsWith("set") && methodName.length() > 3 && nargs == 1
        && returnType.equals(Void.TYPE)) {
      return delegateToFucntionService(objectName, methodName, args, signature);

    }

    return delegateToFucntionService(objectName, methodName, args, signature);

  }

  
   

  /**
   * As this proxy may behave as an notification emitter it delegates to the
   * member NotificationBroadcasterSupport object
   * 
   * @param notification
   */
  private void sendNotification(Object notification) {
    emitter.sendNotification((Notification) notification);
  }

  /**
   * This will get the data from Object state which is replicated across the
   * hidden region FederataionComponent being the carrier.
   * 
   * @param attributeName
   * @return Object
   */
  protected Object delegateToObjectState(String attributeName) throws Throwable {

    
    Object returnObj;
    try {
      FederationComponent fedComp = (FederationComponent) monitoringRegion
          .get(objectName.toString());
      returnObj = fedComp.getValue(attributeName);
    } catch (IllegalArgumentException e) {
      throw new MBeanException(e);
    } catch (Exception e) {
      throw new MBeanException(e);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      throw new MBeanException(new Exception(th.getLocalizedMessage()));
    }
    return returnObj;
  }

  /**
   * It will call the Generic function to execute the method on the remote VM
   * 
   * @param objectName
   *          ObjectName of the MBean
   * @param methodName
   *          method name
   * @param args
   *          arguments to the methods
   * @param signature
   *          signature of the method
   * @return result Object
   */
  protected Object delegateToFucntionService(ObjectName objectName,
      String methodName, Object[] args, String[] signature) throws Throwable {

    Object[] functionArgs = new Object[5];
    functionArgs[0] = objectName;
    functionArgs[1] = methodName;
    functionArgs[2] = signature;
    functionArgs[3] = args;
    functionArgs[4] = member.getName();
    List<Object> result = null;
    try {

      ResultCollector rc = FunctionService.onMember(member).withArgs(
          functionArgs).execute(ManagementConstants.MGMT_FUNCTION_ID);
      result = (List<Object>) rc.getResult();
      // Exceptions of ManagementFunctions
     

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(" Exception while Executing Funtion {}", e.getMessage(), e);
      } 
      //Only in case of Exception caused for Function framework.
      return null;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      if (logger.isDebugEnabled()) {
        logger.debug(" Exception while Executing Funtion {}", th.getMessage(), th);
      }    
      return null;
    }

    return checkErrors(result.get(ManagementConstants.RESULT_INDEX));

  }
  
  private Object checkErrors(Object lastResult) throws Throwable {
   
    
    if (lastResult instanceof MBeanException) {
      // Convert all MBean public API exceptions to MBeanException
      throw (Exception) lastResult;
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
   * The call will delegate to Managed Node for NotificationHub to register a
   * local listener to listen for notification from the MBean
   * 
   * Moreover it will also add the client to local listener list by adding to
   * the contained emitter.
   * 
   * @param proxy
   *          the proxy object
   * @param method
   *          method to be invoked
   * @param args
   *          method arguments
   * @return result value if any
   * @throws Exception
   */
  private Object invokeBroadcasterMethod(Object proxy, Method method,
      Object[] args) throws Throwable {
    final String methodName = method.getName();
    final int nargs = (args == null) ? 0 : args.length;

    final Class[] paramTypes = method.getParameterTypes();
    final String[] signature = new String[paramTypes.length];

    if (methodName.equals("addNotificationListener")) {

      /*
       * The various throws of IllegalArgumentException here should not happen,
       * since we know what the methods in NotificationBroadcaster and
       * NotificationEmitter are.
       */

      if (nargs != 3) {
        final String msg = "Bad arg count to addNotificationListener: " + nargs;
        throw new IllegalArgumentException(msg);
      }
      /*
       * Other inconsistencies will produce ClassCastException below.
       */

      NotificationListener listener = (NotificationListener) args[0];
      NotificationFilter filter = (NotificationFilter) args[1];
      Object handback = args[2];
      emitter.addNotificationListener(listener, filter, handback);
      delegateToFucntionService(objectName, methodName, null, signature);
      return null;

    } else if (methodName.equals("removeNotificationListener")) {
      /*
       * NullPointerException if method with no args, but that shouldn't happen
       * because removeNL does have args.
       */
      NotificationListener listener = (NotificationListener) args[0];

      switch (nargs) {
      case 1:
        emitter.removeNotificationListener(listener);
        /**
         * No need to send listener and filter details to other members.
         * We only need to send a message saying remove the listner registered for this object on your side.
         * Fixes Bug[ #47075 ] 
         */
        delegateToFucntionService(objectName, methodName, null, signature);
        return null;

      case 3:
        NotificationFilter filter = (NotificationFilter) args[1];
        Object handback = args[2];
        emitter.removeNotificationListener(listener, filter, handback);

        delegateToFucntionService(objectName, methodName, null, signature);
        return null;

      default:
        final String msg = "Bad arg count to removeNotificationListener: "
            + nargs;
        throw new IllegalArgumentException(msg);
      }

    } else if (methodName.equals("getNotificationInfo")) {

      if (args != null) {
        throw new IllegalArgumentException("getNotificationInfo has " + "args");
      }
       
      if(!MBeanJMXAdapter.mbeanServer.isRegistered(objectName)){
        return new MBeanNotificationInfo[0];
      }
      
      /**
       * MBean info is delegated to function service as intention is to get the
       * info of the actual mbean rather than the proxy
       */
      
      
      Object obj = delegateToFucntionService(objectName,
          methodName, args, signature);
      if(obj instanceof String){
          return new MBeanNotificationInfo[0];
      }
      MBeanInfo info = (MBeanInfo) obj;
      return info.getNotifications();

    } else {
      throw new IllegalArgumentException("Bad method name: " + methodName);
    }
  }
  
 

  /**
   * Internal implementation of all the generic proxy methods
   * 
   * 
   */
  private class ProxyInterfaceImpl implements ProxyInterface {
    /**
     * last refreshed time of the proxy
     */
    private long lastRefreshedTime;

    /**
     * Constructore
     */
    public ProxyInterfaceImpl() {
      this.lastRefreshedTime = System.currentTimeMillis();
    }

    /**
     * Last refreshed time
     */
    public long getLastRefreshedTime() {
      return lastRefreshedTime;
    }

    /**
     * sets the proxy refresh time
     */
    public void setLastRefreshedTime(long lastRefreshedTime) {
      this.lastRefreshedTime = lastRefreshedTime;
    }

  }

  private boolean shouldDoLocally(Object proxy, Method method) {
    final String methodName = method.getName();
    if ((methodName.equals("hashCode") || methodName.equals("toString"))
        && method.getParameterTypes().length == 0)
      return true;
    if (methodName.equals("equals")
        && Arrays.equals(method.getParameterTypes(),
            new Class[] { Object.class }))
      return true;
    return false;
  }

  private Object doLocally(Object proxy, Method method, Object[] args) {
    final String methodName = method.getName();
    FederationComponent fedComp = (FederationComponent) monitoringRegion
        .get(objectName.toString());
    if (methodName.equals("equals")) {

      return fedComp.equals(args[0]);

    } else if (methodName.equals("toString")) {
      return fedComp.toString();
    } else if (methodName.equals("hashCode")) {
      return fedComp.hashCode();
    }

    throw new RuntimeException("Unexpected method name: " + methodName);
  }

  private MXBeanProxyInvocationHandler findMXBeanProxy(ObjectName objectName,
      Class<?> mxbeanInterface, MBeanProxyInvocationHandler handler)
      throws Throwable {
    MXBeanProxyInvocationHandler proxyRef = mxbeanInvocationRef;

    if (mxbeanInvocationRef == null) {
      synchronized (this) {
        try {
          mxbeanInvocationRef = new MXBeanProxyInvocationHandler(objectName,
              mxbeanInterface, handler);
        } catch (IllegalArgumentException e) {
          String msg = "Cannot make MXBean proxy for "
              + mxbeanInterface.getName() + ": " + e.getMessage();
          throw new IllegalArgumentException(msg, e.getCause());
        }

      }

    }
    return mxbeanInvocationRef;
  }
 
}

