/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package dunit;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Properties;


/**
 * This class provides an abstraction over the environment
 * that is used to run dunit. This will delegate to the hydra
 * or to the standalone dunit launcher as needed.
 * 
 * Any dunit tests that rely on hydra configuration should go
 * through here, so that we can separate them out from depending on hydra
 * and run them on a different VM launching system.
 *   
 * @author dsmith
 *
 */
public abstract class DUnitEnv {
  
  public static DUnitEnv instance = null;
  
  public static final DUnitEnv get() {
    if (instance == null) {
      try {
        // for tests that are still being migrated to the open-source
        // distributed unit test framework  we need to look for this
        // old closed-source dunit environment
        Class clazz = Class.forName("dunit.hydra.HydraDUnitEnv");
        instance = (DUnitEnv)clazz.newInstance();
      } catch (Exception e) {
        throw new Error("Distributed unit test environment is not initialized");
      }
    }
    return instance;
  }
  
  public static void set(DUnitEnv dunitEnv) {
    instance = dunitEnv;
  }
  
  public abstract String getLocatorString();
  
  public abstract String getLocatorAddress();

  public abstract int getLocatorPort();
  
  public abstract Properties getDistributedSystemProperties();

  public abstract int getPid();

  public abstract int getVMID();

  public abstract BounceResult bounce(int pid) throws RemoteException;

  public abstract File getWorkingDirectory(int pid);
  
}
