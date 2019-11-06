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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.version.VersionManager;

public class Master extends UnicastRemoteObject implements MasterRemote {
  private static final long serialVersionUID = 1178600200232603119L;

  private final Registry registry;
  private final ProcessManager processManager;
  private int locatorPort;

  public Master(Registry registry, ProcessManager processManager) throws RemoteException {
    this.processManager = processManager;
    this.registry = registry;
  }

  @Override
  public int getLocatorPort() throws RemoteException {
    return locatorPort;
  }

  public void setLocatorPort(int port) {
    this.locatorPort = port;
  }

  @Override
  public synchronized void signalVMReady() {
    processManager.signalVMReady();
  }

  @Override
  public void ping() {
    // do nothing
  }

  @Override
  public BounceResult bounce(int pid) {
    return bounce(VersionManager.CURRENT_VERSION, pid, false);
  }

  @Override
  public BounceResult bounce(String version, int pid, boolean force) {
    processManager.bounce(version, pid, force);

    try {
      if (!processManager.waitForVMs(DUnitLauncher.STARTUP_TIMEOUT)) {
        throw new RuntimeException(DUnitLauncher.STARTUP_TIMEOUT_MESSAGE);
      }
      RemoteDUnitVMIF remote =
          (RemoteDUnitVMIF) registry.lookup(VM.getVMName(VersionManager.CURRENT_VERSION, pid));
      return new BounceResult(pid, remote);
    } catch (RemoteException | NotBoundException e) {
      throw new RuntimeException("could not lookup name", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed waiting for VM", e);
    }
  }
}
