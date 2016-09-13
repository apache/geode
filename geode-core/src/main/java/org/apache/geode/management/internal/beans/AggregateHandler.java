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
package org.apache.geode.management.internal.beans;

import javax.management.ObjectName;

import org.apache.geode.management.internal.FederationComponent;

/**
 * Internal aggregate handlers could(they can be independent of this interface
 * also as long as they adhere to ProxyAggregator contract) implement this
 * interface.
 * 
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
