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
package org.apache.geode.test.dunit.standalone;

import java.rmi.Naming;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher.MasterRemote;

public class ChildVM {

  private static boolean stopMainLoop = false;

  /**
   * tells the main() loop to exit
   */
  public static void stopVM() {
    stopMainLoop = true;
  }

  private static final Logger logger = LogService.getLogger();

  public static void main(String[] args) throws Throwable {
    try {
      int namingPort = Integer.getInteger(DUnitLauncher.RMI_PORT_PARAM);
      int vmNum = Integer.getInteger(DUnitLauncher.VM_NUM_PARAM);
      String geodeVersion = System.getProperty(DUnitLauncher.VM_VERSION_PARAM);
      int pid = OSProcess.getId();
      logger.info("VM" + vmNum + " is launching" + (pid > 0 ? " with PID " + pid : ""));
      if (!VersionManager.isCurrentVersion(geodeVersion)) {
        logger.info("This VM is using Geode version {}. Version.CURRENT is {} (ordinal={})",
            geodeVersion, Version.CURRENT, Version.CURRENT_ORDINAL);
      }
      MasterRemote holder = (MasterRemote) Naming
          .lookup("//localhost:" + namingPort + "/" + DUnitLauncher.MASTER_PARAM);
      DUnitLauncher.init(holder);
      DUnitLauncher.locatorPort = holder.getLocatorPort();
      final RemoteDUnitVM dunitVM = new RemoteDUnitVM();
      final String name = "//localhost:" + namingPort + "/vm" + vmNum;
      Naming.rebind(name, dunitVM);
      JUnit4DistributedTestCase.initializeBlackboard();
      holder.signalVMReady();
      // This loop is here so this VM will die even if the master is mean killed.
      while (!stopMainLoop) {
        holder.ping();
        Thread.sleep(1000);
      }
    } catch (Throwable t) {
      logger.info("VM is exiting with error", t);
      ExitCode.FATAL.doSystemExit();
    } finally {
      logger.info("VM is exiting");
    }
  }
}
