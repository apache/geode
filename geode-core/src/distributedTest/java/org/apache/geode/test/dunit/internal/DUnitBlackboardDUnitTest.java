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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;


public class DUnitBlackboardDUnitTest extends JUnit4DistributedTestCase {
  @Test
  public void canPassDataBetweenVMs() throws Exception {
    final String MBOX = "myMailbox";
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);

    vm0.invoke("put data in mailbox", () -> getBlackboard().setMailbox(MBOX, "testing"));

    String result = (String) vm1.invoke("get data from mailbox", () -> {
      return getBlackboard().getMailbox(MBOX);
    });

    assertEquals("testing", result);
  }

  @Test
  public void canSignalAnotherVM() throws Exception {
    final String GATE = "myGate";
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);

    vm1.invoke("wait on gate not yet signalled", () -> {
      assertFalse(getBlackboard().isGateSignaled(GATE));
      try {
        getBlackboard().waitForGate(GATE, 1, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // expected
        return;
      } catch (InterruptedException e) {
        fail("unexpected interrupt");
      }
      fail("unexpected success");
    });

    vm0.invoke("signal gate", () -> getBlackboard().signalGate(GATE));

    vm1.invoke("wait on gate not yet signalled", () -> {
      try {
        getBlackboard().waitForGate(GATE, 1, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        fail("unexpected timeout");
      } catch (InterruptedException e) {
        fail("unexpected interrupt");
      }
      // success expected
    });
  }

  @Test
  public void initBlackboardClearsEverything() throws Exception {
    for (int i = 0; i < 100; i++) {
      getBlackboard().setMailbox("MBOX" + i, "value" + i);
      assertEquals("value" + i, getBlackboard().getMailbox("MBOX" + i));
      getBlackboard().signalGate("GATE" + i);
      assertTrue(getBlackboard().isGateSignaled("GATE" + i));
    }
    Host.getHost(0).getVM(1).invoke("clear blackboard", () -> getBlackboard().initBlackboard());

    for (int i = 0; i < 100; i++) {
      assertNull(getBlackboard().getMailbox("MBOX" + i));
      assertFalse(getBlackboard().isGateSignaled("GATE" + i));
    }
  }
}
