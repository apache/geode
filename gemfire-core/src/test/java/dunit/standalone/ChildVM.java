/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package dunit.standalone;

import hydra.HydraRuntimeException;
import hydra.Log;

import java.rmi.Naming;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.logging.LogService;

import dunit.standalone.DUnitLauncher.MasterRemote;

/**
 * @author dsmith
 *
 */
public class ChildVM {
  
  static {
    createHydraLogWriter();
  }
  
  private final static Logger logger = LogService.getLogger();
  
  public static void main(String[] args) throws Throwable {
    try {
      int namingPort = Integer.getInteger(DUnitLauncher.RMI_PORT_PARAM).intValue();
      int vmNum = Integer.getInteger(DUnitLauncher.VM_NUM_PARAM).intValue();
      int pid = OSProcess.getId();
      logger.info("VM" + vmNum + " is launching" + (pid > 0? " with PID " + pid : ""));
      MasterRemote holder = (MasterRemote) Naming.lookup("//localhost:" + namingPort + "/" + DUnitLauncher.MASTER_PARAM);
      DUnitLauncher.init(holder);
      DUnitLauncher.locatorPort = holder.getLocatorPort();
      Naming.rebind("//localhost:" + namingPort + "/vm" + vmNum, new RemoteDUnitVM());
      holder.signalVMReady();
      //This loop is here so this VM will die even if the master is mean killed.
      while(true) {
        holder.ping();
        Thread.sleep(1000);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
  }

  private static void createHydraLogWriter() {
    try {
      Log.createLogWriter("dunit-childvm", "fine");
    } catch (HydraRuntimeException ignore) {
    }
  }
}
