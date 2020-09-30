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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.geode.test.dunit.internal.InternalBlackboard;
import org.apache.geode.test.dunit.internal.InternalBlackboardImpl;

/**
 * DUnitBlackboard provides mailboxes and synchronization gateways for distributed tests.
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
public class DUnitBlackboard implements Blackboard {

  private final InternalBlackboard blackboard;

  public DUnitBlackboard() {
    this(InternalBlackboardImpl.getInstance());
  }

  public DUnitBlackboard(InternalBlackboard blackboard) {
    this.blackboard = blackboard;
  }

  @Override
  public void initBlackboard() {
    try {
      blackboard.initBlackboard();
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  @Override
  public void signalGate(String gateName) {
    try {
      blackboard.signalGate(gateName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  @Override
  public void waitForGate(String gateName)
      throws TimeoutException, InterruptedException {
    waitForGate(gateName, getTimeout().toMinutes(), MINUTES);
  }

  @Override
  public void waitForGate(String gateName, long timeout, TimeUnit units)
      throws TimeoutException, InterruptedException {
    try {
      blackboard.waitForGate(gateName, timeout, units);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  @Override
  public void clearGate(String gateName) {
    try {
      blackboard.clearGate(gateName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  @Override
  public boolean isGateSignaled(String gateName) {
    try {
      return blackboard.isGateSignaled(gateName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  @Override
  public void setMailbox(String boxName, Object value) {
    try {
      blackboard.setMailbox(boxName, value);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  @Override
  public <T> T getMailbox(String boxName) {
    try {
      return blackboard.getMailbox(boxName);
    } catch (RemoteException e) {
      throw new RuntimeException("remote call failed", e);
    }
  }

  public InternalBlackboard internal() {
    return blackboard;
  }
}
