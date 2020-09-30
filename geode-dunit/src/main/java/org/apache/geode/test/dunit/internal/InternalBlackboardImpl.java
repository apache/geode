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

import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InternalBlackboardImpl extends UnicastRemoteObject implements InternalBlackboard {

  private static InternalBlackboard blackboard;

  private final Map<String, Boolean> gates = new ConcurrentHashMap<>();
  private final Map<String, Serializable> mailboxes = new ConcurrentHashMap<>();

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public InternalBlackboardImpl() throws RemoteException {
    // nothing
  }

  /**
   * Creates a singleton event listeners blackboard.
   */
  public static synchronized InternalBlackboard getInstance() {
    if (blackboard == null) {
      try {
        initialize();
      } catch (AlreadyBoundException abx) {
        // ignored
      } catch (Exception e) {
        throw new RuntimeException("failed to initialize blackboard", e);
      }
    }
    return blackboard;
  }

  private static synchronized void initialize()
      throws AlreadyBoundException, MalformedURLException, RemoteException {
    if (blackboard == null) {
      System.out.println(
          DUnitLauncher.RMI_PORT_PARAM + "=" + System.getProperty(DUnitLauncher.RMI_PORT_PARAM));
      int namingPort = Integer.getInteger(DUnitLauncher.RMI_PORT_PARAM);
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
    gates.clear();
    mailboxes.clear();
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
      throws InterruptedException, RemoteException, TimeoutException {
    long giveupTime = System.currentTimeMillis() + MILLISECONDS.convert(timeout, units);
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
    return gate != null && gate;
  }

  @Override
  public <T> void setMailbox(String boxName, T value) {
    mailboxes.put(boxName, (Serializable) value);
  }

  @Override
  public <T> T getMailbox(String boxName) {
    return uncheckedCast(mailboxes.get(boxName));
  }

  @Override
  public void ping() throws RemoteException {
    // no-op
  }

  @Override
  public Map<String, Boolean> gates() {
    return unmodifiableMap(gates);
  }

  @Override
  public Map<String, Serializable> mailboxes() {
    return unmodifiableMap(mailboxes);
  }

  @Override
  public void putGates(Map<String, Boolean> gates) {
    this.gates.putAll(gates);
  }

  @Override
  public void putMailboxes(Map<String, Serializable> mailboxes) {
    this.mailboxes.putAll(mailboxes);
  }
}
