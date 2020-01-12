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
package org.apache.geode.test.dunit;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.geode.test.dunit.internal.InternalBlackboard;
import org.apache.geode.test.dunit.internal.InternalBlackboardImpl;

/**
 * DUnitBlackboard provides mailboxes and synchronization gateways for distributed unit tests.
 *
 * <p>
 * Tests may use the blackboard to pass objects and status between JVMs with mailboxes instead of
 * using static variables in classes. The caveat being that the objects will be serialized using
 * Java serialization.
 *
 * <p>
 * Gates may be used to synchronize operations between unit test JVMs. Combined with Awaitility
 * these can be used to test for conditions being met, actions having happened, etc.
 *
 * <p>
 * Look for references to the given methods in your IDE for examples.
 */
public class DUnitBlackboard {

  private InternalBlackboard blackboard;

  public DUnitBlackboard() {
    blackboard = InternalBlackboardImpl.getInstance();
  }

  /**
   * resets the blackboard
   */
  public void initBlackboard() {
    try {
      blackboard.initBlackboard();
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  /**
   * signals a boolean gate
   */
  public void signalGate(String gateName) {
    // System.out.println(Thread.currentThread().getName()+": signaling gate " + gateName);
    try {
      blackboard.signalGate(gateName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  /**
   * wait for a gate to be signaled
   */
  public void waitForGate(String gateName, long timeout, TimeUnit units)
      throws TimeoutException, InterruptedException {
    // System.out.println(Thread.currentThread().getName()+": waiting for gate " + gateName);
    try {
      blackboard.waitForGate(gateName, timeout, units);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  /**
   * clear a gate
   */
  public void clearGate(String gateName) {
    try {
      blackboard.clearGate(gateName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  /**
   * test to see if a gate has been signeled
   */
  public boolean isGateSignaled(String gateName) {
    try {
      return blackboard.isGateSignaled(gateName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  /**
   * put an object into a mailbox slot. The object must be java-serializable
   */
  public void setMailbox(String boxName, Object value) {
    try {
      blackboard.setMailbox(boxName, value);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  /**
   * retrieve an object from a mailbox slot
   */
  public <T> T getMailbox(String boxName) {
    try {
      return blackboard.getMailbox(boxName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }
}
