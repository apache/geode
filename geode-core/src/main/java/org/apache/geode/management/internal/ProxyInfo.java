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

import javax.management.ObjectName;

/**
 * Proxy info class holds details about proxy for quicker access during several Management
 * operations
 *
 */
public class ProxyInfo {

  /**
   * Proxy Interface
   */
  private Class proxyInterface;
  /**
   * proxy instance
   */
  private final Object proxyInstance;

  /**
   * JMX name of proxy
   */
  private final ObjectName objectName;


  /**
   * public constructor
   *
   */
  public ProxyInfo(Class proxyInterface, Object proxyInstance, ObjectName objectName) {
    this.proxyInstance = proxyInstance;
    this.proxyInterface = proxyInterface;
    this.objectName = objectName;

  }

  /**
   * get the proxy instance
   *
   */
  public Object getProxyInstance() {
    return proxyInstance;
  }


  /**
   * get MBean name
   *
   */
  public ObjectName getObjectName() {
    return objectName;
  }

  public Class getProxyInterface() {
    return proxyInterface;
  }

  public void setProxyInterface(Class proxyInterface) {
    this.proxyInterface = proxyInterface;
  }


}
