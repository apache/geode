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
package org.apache.geode.distributed;

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.geode.internal.Assert;
import org.apache.geode.test.dunit.internal.DUnitLauncher;


public class DistributedLockBlackboardImpl extends UnicastRemoteObject
    implements DistributedLockBlackboard {
  public static int Count;
  public static int IsLocked;

  public static DistributedLockBlackboard blackboard;

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public DistributedLockBlackboardImpl() throws RemoteException {
    super();
  }

  /**
   * Creates a singleton event listeners blackboard.
   */
  public static DistributedLockBlackboard getInstance() throws Exception {
    if (blackboard == null) {
      initialize();
    }
    return blackboard;
  }

  private static synchronized void initialize() throws Exception {
    if (blackboard == null) {
      System.out.println(
          DUnitLauncher.RMI_PORT_PARAM + "=" + System.getProperty(DUnitLauncher.RMI_PORT_PARAM));
      int namingPort = Integer.getInteger(DUnitLauncher.RMI_PORT_PARAM).intValue();
      String name = "//localhost:" + namingPort + "/" + "DistributedLockBlackboard";
      try {
        blackboard = (DistributedLockBlackboard) Naming.lookup(name);
      } catch (NotBoundException e) {
        // create the master blackboard in this VM
        blackboard = new DistributedLockBlackboardImpl();
        Naming.bind(name, blackboard);
      }
    }
  }

  @Override
  public synchronized void initCount() {
    Count = 0;
  }

  @Override
  public synchronized void incCount() {
    Count = Count + 1;
  }


  @Override
  public synchronized long getCount() {
    return Count;
  }

  @Override
  public synchronized void setIsLocked(boolean isLocked) {
    if (isLocked) {
      if (IsLocked < 1) {
        IsLocked = 1;
      }
    } else {
      if (IsLocked > 0) {
        IsLocked = 0;
      }
    }
  }

  @Override
  public synchronized boolean getIsLocked() {
    long isLocked = IsLocked;
    Assert.assertTrue(isLocked == 0 || isLocked == 1,
        "DistributedLockBlackboard internal error - IsLocked is " + isLocked);
    return isLocked == 1;
  }

}
