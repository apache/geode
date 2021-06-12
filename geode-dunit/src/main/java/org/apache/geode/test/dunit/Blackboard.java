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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Blackboard provides mailboxes and synchronization gateways for distributed tests.
 *
 * <p>
 * Tests may use the blackboard to pass objects and status between JVMs with mailboxes instead of
 * using static variables in classes. The caveat being that the objects will be serialized using
 * Java serialization.
 *
 * <p>
 * Gates may be used to synchronize operations between distributed test JVMs. Combined with
 * Awaitility these can be used to test for conditions being met, actions having happened, etc.
 *
 * <p>
 * Look for references to the given methods in your IDE for examples.
 */
public interface Blackboard {

  /**
   * Resets the blackboard.
   */
  void initBlackboard();

  /**
   * Signals a boolean gate.
   */
  void signalGate(String gateName);

  /**
   * Waits at most {@link GeodeAwaitility#getTimeout()} for a gate to be signaled.
   */
  void waitForGate(String gateName) throws TimeoutException, InterruptedException;

  /**
   * Waits at most the specified timeout for a gate to be signaled.
   */
  void waitForGate(String gateName, long timeout, TimeUnit units)
      throws TimeoutException, InterruptedException;

  /**
   * Clears a gate.
   */
  void clearGate(String gateName);

  /**
   * Checks to see if a gate has been signaled.
   */
  boolean isGateSignaled(String gateName);

  /**
   * Puts an object into a mailbox slot. The object must be java-serializable.
   */
  <T> void setMailbox(String boxName, T value);

  /**
   * Retrieves an object from a mailbox slot.
   */
  <T> T getMailbox(String boxName);
}
