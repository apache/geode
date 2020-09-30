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
package org.apache.geode.test.dunit.rules;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.test.dunit.Blackboard;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.InternalBlackboard;
import org.apache.geode.test.dunit.internal.InternalBlackboardImpl;

/**
 * DistributedBlackboard provides mailboxes and synchronization gateways for distributed tests.
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
@SuppressWarnings({"serial", "unused"})
public class DistributedBlackboard extends AbstractDistributedRule implements Blackboard {

  private static final AtomicReference<DUnitBlackboard> BLACKBOARD = new AtomicReference<>();
  private static final AtomicReference<InternalBlackboard> INTERNAL = new AtomicReference<>();

  private final Map<Integer, Map<String, Boolean>> keepGates = new ConcurrentHashMap<>();
  private final Map<Integer, Map<String, Serializable>> keepMailboxes = new ConcurrentHashMap<>();

  @Override
  protected void before() {
    invoker().invokeInEveryVMAndController(() -> invokeBefore());
  }

  @Override
  protected void after() throws Throwable {
    invoker().invokeInEveryVMAndController(() -> invokeAfter());
  }

  @Override
  protected void afterCreateVM(VM vm) {
    vm.invoke(() -> invokeBefore());
  }

  @Override
  protected void beforeBounceVM(VM vm) {
    keepGates.put(vm.getId(), vm.invoke(() -> INTERNAL.get().gates()));
    keepMailboxes.put(vm.getId(), vm.invoke(() -> INTERNAL.get().mailboxes()));
  }

  @Override
  protected void afterBounceVM(VM vm) {
    Map<String, Boolean> keepGatesForVM = keepGates.remove(vm.getId());
    Map<String, Serializable> keepMailboxesForVM = keepMailboxes.remove(vm.getId());

    vm.invoke(() -> {
      invokeBefore();
      INTERNAL.get().putGates(keepGatesForVM);
      INTERNAL.get().putMailboxes(keepMailboxesForVM);
    });
  }

  private void invokeBefore() {
    InternalBlackboard internalBlackboard = InternalBlackboardImpl.getInstance();
    INTERNAL.set(internalBlackboard);
    BLACKBOARD.set(new DUnitBlackboard(internalBlackboard));
  }

  private void invokeAfter() {
    BLACKBOARD.set(null);
    INTERNAL.set(null);
  }

  @Override
  public void initBlackboard() {
    BLACKBOARD.get().initBlackboard();
  }

  @Override
  public void signalGate(String gateName) {
    BLACKBOARD.get().signalGate(gateName);
  }

  @Override
  public void waitForGate(String gateName) throws TimeoutException, InterruptedException {
    BLACKBOARD.get().waitForGate(gateName);
  }

  @Override
  public void waitForGate(String gateName, long timeout, TimeUnit units)
      throws TimeoutException, InterruptedException {
    BLACKBOARD.get().waitForGate(gateName, timeout, units);
  }

  @Override
  public void clearGate(String gateName) {
    BLACKBOARD.get().clearGate(gateName);
  }

  @Override
  public boolean isGateSignaled(String gateName) {
    return BLACKBOARD.get().isGateSignaled(gateName);
  }

  @Override
  public <T> void setMailbox(String boxName, T value) {
    BLACKBOARD.get().setMailbox(boxName, value);
  }

  @Override
  public <T> T getMailbox(String boxName) {
    return BLACKBOARD.get().getMailbox(boxName);
  }
}
