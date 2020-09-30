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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.geode.test.dunit.VM;

@SuppressWarnings("serial")
public class DUnitBlackboardDUnitTest extends JUnit4DistributedTestCase {

  @Test
  public void canPassDataBetweenVMs() {
    final String MBOX = "myMailbox";
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    vm0.invoke("put data in mailbox", () -> getBlackboard().setMailbox(MBOX, "testing"));

    String result = vm1.invoke("get data from mailbox", () -> getBlackboard().getMailbox(MBOX));

    assertThat(result).isEqualTo("testing");
  }

  @Test
  public void canSignalAnotherVM() {
    final String GATE = "myGate";
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    vm1.invoke("wait on gate not yet signalled", () -> {
      assertThat(getBlackboard().isGateSignaled(GATE)).isFalse();

      Throwable thrown = catchThrowable(() -> {
        getBlackboard().waitForGate(GATE, 1, SECONDS);
      });

      assertThat(thrown).isInstanceOf(TimeoutException.class);
    });

    vm0.invoke("signal gate", () -> getBlackboard().signalGate(GATE));

    vm1.invoke("wait on gate not yet signalled",
        () -> getBlackboard().waitForGate(GATE, 1, SECONDS));
  }

  @Test
  public void initBlackboardClearsEverything() {
    for (int i = 0; i < 100; i++) {
      getBlackboard().setMailbox("MBOX" + i, "value" + i);
      assertThat((Object) getBlackboard().getMailbox("MBOX" + i)).isEqualTo("value" + i);

      getBlackboard().signalGate("GATE" + i);
      assertThat(getBlackboard().isGateSignaled("GATE" + i)).isTrue();
    }

    getVM(1).invoke("clear blackboard", () -> getBlackboard().initBlackboard());

    for (int i = 0; i < 100; i++) {
      assertThat((Object) getBlackboard().getMailbox("MBOX" + i)).isNull();
      assertThat(getBlackboard().isGateSignaled("GATE" + i)).isFalse();
    }
  }
}
