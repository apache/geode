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

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.geode.test.dunit.standalone.DUnitLauncher;


public class InternalBlackboardImpl extends UnicastRemoteObject implements InternalBlackboard {
  public static InternalBlackboard blackboard;

  private Map<String, Boolean> gates = new ConcurrentHashMap<>();

  private Map<String, Object> mailboxes = new ConcurrentHashMap();


  /**
   * Zero-arg constructor for remote method invocations.
   */
  public InternalBlackboardImpl() throws RemoteException {
    super();
  }

  /**
   * Creates a singleton event listeners blackboard.
   */
  public static InternalBlackboard getInstance() {
    if (blackboard == null) {
      try {
        initialize();
      } catch (Exception e) {
        throw new RuntimeException("failed to initialize blackboard", e);
      }
    }
    return blackboard;
  }

  private static synchronized void initialize() throws Exception {
    if (blackboard == null) {
      System.out.println(
          DUnitLauncher.RMI_PORT_PARAM + "=" + System.getProperty(DUnitLauncher.RMI_PORT_PARAM));
      int namingPort = Integer.getInteger(DUnitLauncher.RMI_PORT_PARAM).intValue();
      String name = "//localhost:" + namingPort + "/" + "InternalBlackboard";
      try {
        blackboard = (InternalBlackboard) Naming.lookup(name);
      } catch (NotBoundException e) {
        // create the master blackboard in this VM
        blackboard = new InternalBlackboardImpl();
        Naming.bind(name, blackboard);
      }
    }
  }

  @Override
  public void initBlackboard() throws RemoteException {
    this.gates.clear();
    this.mailboxes.clear();
  }

  @Override
  public void clearGate(final String gateName) throws RemoteException {
    gates.remove(gateName);
  }

  @Override
  public void signalGate(final String gateName) throws RemoteException {
    gates.put(gateName, Boolean.TRUE);
  }

  @Override
  public void waitForGate(final String gateName, final long timeout, final TimeUnit units)
      throws RemoteException, TimeoutException, InterruptedException {
    long giveupTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, units);
    while (System.currentTimeMillis() < giveupTime) {
      Boolean gate = gates.get(gateName);
      if (gate != null && gate) {
        return;
      }
      Thread.sleep(50L);
    }
    throw new TimeoutException();
  }

  @Override
  public boolean isGateSignaled(final String gateName) {
    Boolean gate = gates.get(gateName);
    return (gate != null && gate);
  }

  @Override
  public void setMailbox(String boxName, Object value) {
    mailboxes.put(boxName, value);
  }

  @Override
  public Object getMailbox(String boxName) {
    return mailboxes.get(boxName);
  }

  @Override
  public void ping() throws RemoteException {
    // no-op
  }


}
