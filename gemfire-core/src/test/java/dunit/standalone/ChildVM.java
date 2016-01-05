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
  
  private static boolean stopMainLoop = false;
  
  /**
   * tells the main() loop to exit
   */
  public static void stopVM() {
    stopMainLoop = true;
  }
  
  static {
    createHydraLogWriter();
  }
  
  private final static Logger logger = LogService.getLogger();
  private static RemoteDUnitVM dunitVM;
  
  public static void main(String[] args) throws Throwable {
    try {
      int namingPort = Integer.getInteger(DUnitLauncher.RMI_PORT_PARAM).intValue();
      int vmNum = Integer.getInteger(DUnitLauncher.VM_NUM_PARAM).intValue();
      int pid = OSProcess.getId();
      logger.info("VM" + vmNum + " is launching" + (pid > 0? " with PID " + pid : ""));
      MasterRemote holder = (MasterRemote) Naming.lookup("//localhost:" + namingPort + "/" + DUnitLauncher.MASTER_PARAM);
      DUnitLauncher.init(holder);
      DUnitLauncher.locatorPort = holder.getLocatorPort();
      dunitVM = new RemoteDUnitVM();
      Naming.rebind("//localhost:" + namingPort + "/vm" + vmNum, dunitVM);
      holder.signalVMReady();
      //This loop is here so this VM will die even if the master is mean killed.
      while (!stopMainLoop) {
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
