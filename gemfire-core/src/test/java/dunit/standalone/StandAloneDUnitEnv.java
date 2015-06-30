/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package dunit.standalone;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Properties;

import dunit.BounceResult;
import dunit.DUnitEnv;
import dunit.standalone.DUnitLauncher.MasterRemote;

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
