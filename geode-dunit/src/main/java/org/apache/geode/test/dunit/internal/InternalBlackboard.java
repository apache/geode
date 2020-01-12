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

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DUnitBlackboard provides mailboxes and synchronization gateways for distributed unit tests.
 * <p>
 * Tests may use the blackboard to pass objects and status between JVMs with mailboxes instead of
 * using static variables in classes. The caveat being that the objects will be serialized using
 * Java serialization.
 * <p>
 * Gates may be used to synchronize operations between unit test JVMs. Combined with Awaitility
 * these can be used to test for conditions being met, actions having happened, etc.
 * <p>
 * Look for references to the given methods in your IDE for examples.
 */
public interface InternalBlackboard extends Remote, Serializable {
  /**
   * resets the blackboard
   */
  void initBlackboard() throws RemoteException;

  /**
   * signals a boolean gate
   */
  void signalGate(String gateName) throws RemoteException;

  /**
   * wait for a gate to be signaled
   */
  void waitForGate(String gateName, long timeout, TimeUnit units)
      throws RemoteException, TimeoutException, InterruptedException;

  /**
   * clears a gate
   */
  void clearGate(String gateName) throws RemoteException;

  /**
   * test to see if a gate has been signeled
   */
  boolean isGateSignaled(String gateName) throws RemoteException;

  /**
   * put an object into a mailbox slot. The object must be java-serializable
   */
  void setMailbox(String boxName, Object value) throws RemoteException;

  /**
   * retrieve an object from a mailbox slot
   */
  <T> T getMailbox(String boxName) throws RemoteException;

  /**
   * ping the blackboard to make sure it's there
   */
  void ping() throws RemoteException;

}
