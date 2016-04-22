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
package com.gemstone.gemfire.test.dunit.standalone;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Properties;

import com.gemstone.gemfire.test.dunit.DUnitEnv;
import com.gemstone.gemfire.test.dunit.standalone.DUnitLauncher.MasterRemote;

public class StandAloneDUnitEnv extends DUnitEnv {

  private MasterRemote master;

  public StandAloneDUnitEnv(MasterRemote master) {
    this.master = master;
  }

  @Override
  public String getLocatorString() {
    return DUnitLauncher.getLocatorString();
  }

  @Override
  public String getLocatorAddress() {
    return "localhost";
  }
  
  @Override
  public int getLocatorPort() {
    return DUnitLauncher.locatorPort;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    return DUnitLauncher.getDistributedSystemProperties();
  }

  @Override
  public int getPid() {
    return Integer.getInteger(DUnitLauncher.VM_NUM_PARAM, -1).intValue();
  }

  @Override
  public int getVMID() {
    return getPid();
  }

  @Override
  public BounceResult bounce(int pid) throws RemoteException {
    return master.bounce(pid);
  }

  @Override
  public File getWorkingDirectory(int pid) {
    return ProcessManager.getVMDir(pid);
  }

}
