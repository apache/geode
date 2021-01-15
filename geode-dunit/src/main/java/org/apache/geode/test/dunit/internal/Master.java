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
package org.apache.geode.test.dunit.internal;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


public class Master extends UnicastRemoteObject implements MasterRemote {
  private static final long serialVersionUID = 1178600200232603119L;

  private final Registry registry;
  private final ProcessManager processManager;


  public Master(Registry registry, ProcessManager processManager) throws RemoteException {
    this.processManager = processManager;
    this.registry = registry;
  }

  @Override
  public int getLocatorPort() throws RemoteException {
    return DUnitLauncher.locatorPort;
  }

  @Override
  public synchronized void signalVMReady() {
    processManager.signalVMReady();
  }

  @Override
  public void ping() {
    // do nothing
  }

}
