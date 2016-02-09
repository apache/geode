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
package com.gemstone.gemfire.internal.cache.wan.concurrent;

import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderOperationsDUnitTest;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author skumar
 *
 */
public class ConcurrentSerialGatewaySenderOperationsDUnitTest  extends SerialGatewaySenderOperationsDUnitTest {

  private static final long serialVersionUID = 1L;

  public ConcurrentSerialGatewaySenderOperationsDUnitTest(String name) {
    super(name);
  }

  protected void createSenderVM5() {
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 10, false, true, null, true, 5, OrderPolicy.KEY ));
  }

  protected void createSenderVM4() {
    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 10, false, true, null, true, 5, OrderPolicy.KEY ));
  }

  protected void validateQueueClosedVM4() {
    vm4.invoke(() -> WANTestBase.validateQueueClosedForConcurrentSerialGatewaySender( "ln"));
  }
  
  private void validateQueueContents(VM vm, String site, int size) {
    vm.invoke(() -> WANTestBase.validateQueueContentsForConcurrentSerialGatewaySender( site, size));
  }

  public static void verifySenderPausedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    assertTrue(sender.isPaused());
  }

  public static void verifySenderResumedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    assertFalse(sender.isPaused());
    assertTrue(sender.isRunning());
  }

  public static void verifySenderStoppedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    assertFalse(sender.isRunning());
    assertFalse(sender.isPaused());
  }

  public static void verifyGatewaySenderOperations(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertFalse(sender.isPaused());
    assertFalse(((AbstractGatewaySender)sender).isRunning());
    sender.pause();
    sender.resume();
    sender.stop();
  }
}
