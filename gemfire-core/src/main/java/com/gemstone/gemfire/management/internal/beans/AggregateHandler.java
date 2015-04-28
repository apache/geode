/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import javax.management.ObjectName;

import com.gemstone.gemfire.management.internal.FederationComponent;

/**
 * Internal aggregate handlers could(they can be independent of this interface
 * also as long as they adhere to ProxyAggregator contract) implement this
 * interface.
 * 
 * @author rishim
 * 
 */

public interface AggregateHandler {

  /**
   * 
   * @param objectName
   *          name of the proxy object
   * @param interfaceClass
   *          interface class of the proxy object.
   * @param proxyObject
   *          actual reference of the proxy.
   * @param newVal
   *          new value of the Proxy
   */
  public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal);

  /**
   * 
   * @param objectName
   *          name of the proxy object
   * @param interfaceClass
   *          interface class of the proxy object.
   * @param proxyObject
   *          actual reference of the proxy.
   * @param oldVal
   *          old value of the Proxy
   */
  public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent oldVal);

  /**
   * 
   * @param objectName
   *          name of the proxy object
   * @param interfaceClass
   *          interface class of the proxy object.
   * @param proxyObject
   *          actual reference of the proxy.
   * @param newVal
   *          new value of the Proxy
   * @param oldVal
   *          old value of the proxy
   */
  public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal, FederationComponent oldVal);
  
  /**
   * 
   * @param objectName
   *          name of the proxy object
   * @param interfaceClass
   *          interface class of the proxy object.
   * @param proxyObject
   *          actual reference of the proxy.
   * @param newVal
   *          new value of the Proxy
   */
  public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal);

}
