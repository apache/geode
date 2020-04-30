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
 */
public class ProxyInfo {

  private final Class proxyInterface;
  private final Object proxyInstance;
  private final ObjectName objectName;

  public ProxyInfo(Class proxyInterface, Object proxyInstance, ObjectName objectName) {
    this.proxyInterface = proxyInterface;
    this.proxyInstance = proxyInstance;
    this.objectName = objectName;
  }

  public Class getProxyInterface() {
    return proxyInterface;
  }

  public Object getProxyInstance() {
    return proxyInstance;
  }

  public ObjectName getObjectName() {
    return objectName;
  }

  @Override
  public String toString() {
    return "ProxyInfo{" +
        "proxyInterface=" + proxyInterface +
        ", proxyInstance=" + proxyInstance +
        ", objectName=" + objectName +
        '}';
  }
}