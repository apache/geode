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

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.test.version.VmConfiguration;

class DUnitHost extends Host {
  private static final long serialVersionUID = -8034165624503666383L;

  private final transient VM debuggingVM;
  private final transient ProcessManager processManager;
  private final transient VMEventNotifier vmEventNotifier;

  DUnitHost(String hostName, ProcessManager processManager, VMEventNotifier vmEventNotifier)
      throws RemoteException {
    super(hostName, vmEventNotifier);
    debuggingVM = new VM(this, VmConfiguration.current(), -1, new RemoteDUnitVM(0), null, null);
    this.processManager = processManager;
    this.vmEventNotifier = vmEventNotifier;
  }

  public void init(int numVMs, boolean launchLocator)
      throws RemoteException, NotBoundException, InterruptedException {
    for (int i = 0; i < numVMs; i++) {
      RemoteDUnitVMIF remote = processManager.getStub(i);
      ProcessHolder processHolder = processManager.getProcessHolder(i);
      addVM(i, VmConfiguration.current(), remote, processHolder, processManager);
    }

    if (launchLocator) {
      addLocator(DUnitLauncher.LOCATOR_VM_NUM, processManager.getStub(DUnitLauncher.LOCATOR_VM_NUM),
          processManager.getProcessHolder(DUnitLauncher.LOCATOR_VM_NUM), processManager);
    }

    addHost(this);
  }

  /**
   * Retrieves one of this host's VMs based on the specified VM ID. This will not bounce VM to a
   * different version. It will only get the current running VM or launch a new one if not already
   * launched.
   *
   * @param n ID of the requested VM; a value of <code>-1</code> will return the controller VM,
   *        which may be useful for debugging.
   * @return VM for the requested VM ID.
   */
  @Override
  public VM getVM(int n) {
    if (n < getVMCount() && n != DUnitLauncher.DEBUGGING_VM_NUM) {
      VM current = super.getVM(n);
      return getVM(current.getVersion(), n);
    } else {
      return getVM(VersionManager.CURRENT_VERSION, n);
    }
  }

  @Override
  public VM getVM(String geodeVersion, int whichVM) {
    return getVM(VmConfiguration.forGeodeVersion(geodeVersion), whichVM);
  }

  @Override
  public VM getVM(VmConfiguration configuration, int whichVM) {
    if (whichVM == DUnitLauncher.DEBUGGING_VM_NUM) {
      // for ease of debugging, pass -1 to get the local VM
      return debuggingVM;
    }

    if (whichVM < getVMCount()) {
      VM current = super.getVM(whichVM);
      if (!current.getConfiguration().equals(configuration)) {
        current.bounce(configuration, false);
      }
      return current;
    }

    int oldVMCount = getVMCount();
    if (whichVM >= oldVMCount) {
      // If we don't have a VM with that number, dynamically create it.
      try {
        // first fill in any gaps, to keep the superclass, Host, happy
        for (int i = oldVMCount; i < whichVM; i++) {
          processManager.launchVM(i);
        }
        processManager.waitForVMs(DUnitLauncher.STARTUP_TIMEOUT);

        for (int i = oldVMCount; i < whichVM; i++) {
          addVM(i, VmConfiguration.current(), processManager.getStub(i),
              processManager.getProcessHolder(i), processManager);
        }

        // now create the one we really want
        processManager.launchVM(configuration, whichVM, false, 0);
        processManager.waitForVMs(DUnitLauncher.STARTUP_TIMEOUT);
        addVM(whichVM, configuration, processManager.getStub(whichVM),
            processManager.getProcessHolder(whichVM), processManager);

      } catch (IOException | InterruptedException | NotBoundException e) {
        throw new RuntimeException("Could not dynamically launch vm + " + whichVM, e);
      }
    }

    return super.getVM(whichVM);
  }
}
