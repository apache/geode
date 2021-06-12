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
package org.apache.geode.test.dunit.rules.tests;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@SuppressWarnings({"serial", "CodeBlock2Expr"})
public class DistributedBlackboardDistributedTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();
  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Test
  public void canPassDataBetweenVMs() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    vm0.invoke("put data in mailbox", () -> blackboard.setMailbox(mailbox(), value()));

    String result = vm1.invoke("get data from mailbox", () -> blackboard.getMailbox(mailbox()));

    assertThat(result).isEqualTo(value());
  }

  @Test
  public void canSignalAnotherVM() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    vm1.invoke("wait on gate not yet signalled", () -> {
      assertThat(blackboard.isGateSignaled(gate())).isFalse();

      Throwable thrown = catchThrowable(() -> {
        blackboard.waitForGate(gate(), 1, SECONDS);
      });

      assertThat(thrown).isInstanceOf(TimeoutException.class);
    });

    vm0.invoke("signal gate", () -> blackboard.signalGate(gate()));

    vm1.invoke("wait on gate not yet signalled", () -> blackboard.waitForGate(gate(), 1, SECONDS));
  }

  @Test
  public void initBlackboardClearsEverything() {
    for (int i = 0; i < 100; i++) {
      blackboard.setMailbox(mailbox(i), value(i));
      assertThat((Object) blackboard.getMailbox(mailbox(i))).isEqualTo(value(i));

      blackboard.signalGate(gate(i));
      assertThat(blackboard.isGateSignaled(gate(i))).isTrue();
    }

    getVM(1).invoke("clear blackboard", () -> blackboard.initBlackboard());

    for (int i = 0; i < 100; i++) {
      assertThat((Object) blackboard.getMailbox(mailbox(i))).isNull();
      assertThat(blackboard.isGateSignaled(gate(i))).isFalse();
    }
  }

  @Test
  public void getMailbox_returnsValueFromSameVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value()));

    getVM(0).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    });
  }

  @Test
  public void getMailbox_returnsValueFromOtherVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value()));

    getVM(1).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    });
  }

  @Test
  public void setMailbox_overwrites_valueFromSameVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(1)));
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(2)));

    getVM(0).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value(2));
    });
  }

  @Test
  public void setMailbox_overwrites_valueFromOtherVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(1)));
    getVM(1).invoke(() -> blackboard.setMailbox(mailbox(), value(2)));

    getVM(2).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value(2));
    });
  }

  @Test
  public void getMailbox_returnsValueFromSameVM_afterBouncingVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value()));

    getVM(0).bounceForcibly();

    getVM(0).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    });
  }

  @Test
  public void getMailbox_returnsValueFromOtherVM_afterBouncingVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value()));

    getVM(0).bounceForcibly();

    getVM(1).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    });
  }

  @Test
  public void setMailbox_overwrites_valueFromSameVM_afterBouncingVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(1)));
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(2)));

    getVM(0).bounceForcibly();

    getVM(0).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox(1))).isEqualTo(value(2));
    });
  }

  @Test
  public void setMailbox_overwrites_valueFromOtherVM_afterBouncingFirstVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(1)));
    getVM(1).invoke(() -> blackboard.setMailbox(mailbox(), value(2)));

    getVM(0).bounceForcibly();

    getVM(2).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value(2));
    });
  }

  @Test
  public void setMailbox_overwrites_valueFromOtherVM_afterBouncingSecondVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(1)));
    getVM(1).invoke(() -> blackboard.setMailbox(mailbox(), value(2)));

    getVM(1).bounceForcibly();

    getVM(2).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value(2));
    });
  }

  @Test
  public void setMailbox_overwrites_valueFromOtherVM_afterBouncingBothVMs() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value(1)));
    getVM(1).invoke(() -> blackboard.setMailbox(mailbox(), value(2)));

    getVM(0).bounceForcibly();
    getVM(1).bounceForcibly();

    getVM(2).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value(2));
    });
  }

  @Test
  public void getMailbox_returnsValueFromSameVM_afterBouncingEveryVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value()));

    getVM(0).bounceForcibly();
    getVM(1).bounceForcibly();
    getVM(2).bounceForcibly();
    getVM(3).bounceForcibly();

    getVM(0).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    });
  }

  @Test
  public void getMailbox_returnsValueFromOtherVM_afterBouncingEveryVM() {
    getVM(0).invoke(() -> blackboard.setMailbox(mailbox(), value()));

    getVM(0).bounceForcibly();
    getVM(1).bounceForcibly();
    getVM(2).bounceForcibly();
    getVM(3).bounceForcibly();

    getVM(1).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    });
  }

  @Test
  public void getMailbox_returnsValueFromControllerVM_afterBouncingEveryVM() {
    blackboard.setMailbox(mailbox(), value());

    getVM(0).bounceForcibly();
    getVM(1).bounceForcibly();
    getVM(2).bounceForcibly();
    getVM(3).bounceForcibly();

    getVM(3).invoke(() -> {
      assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    });
  }

  @Test
  public void getMailbox_returnsValueInControllerVM_afterBouncingEveryVM() {
    blackboard.setMailbox(mailbox(), value());

    getVM(0).bounceForcibly();
    getVM(1).bounceForcibly();
    getVM(2).bounceForcibly();
    getVM(3).bounceForcibly();

    assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
  }

  @Test
  public void getMailbox_returnsValueInEveryVM() {
    blackboard.setMailbox(mailbox(), value());

    assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
    for (VM vm : asList(getController(), getVM(0), getVM(1), getVM(2), getVM(3))) {
      vm.invoke(() -> {
        assertThat((Object) blackboard.getMailbox(mailbox())).isEqualTo(value());
      });
    }
  }

  private String mailbox() {
    return value("mailbox", 1);
  }

  private String value() {
    return value("value", 1);
  }

  private String gate() {
    return value("gate", 1);
  }

  private String mailbox(int count) {
    return value("mailbox", count);
  }

  private String value(int count) {
    return value("value", count);
  }

  private String gate(int count) {
    return value("gate", count);
  }

  private String value(String prefix, int count) {
    return prefix + "-" + testName.getMethodName() + "-" + count;
  }
}
