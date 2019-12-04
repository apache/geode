/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.dunit.rules.tests;

import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.Closeable;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings({"serial", "unused"})
public class DistributedReferenceTest {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Test
  public void accessesAutoCloseableInLocalVm() {
    runTestWithValidation(SetAutoCloseableInLocalVm.class);
  }

  @Test
  public void closesAutoCloseableInLocalVm() throws Exception {
    runTestWithValidation(SetAutoCloseableInLocalVm.class);

    verify(SetAutoCloseableInLocalVm.autoCloseable).close();
  }

  @Test
  public void doesNotAutoCloseIfAutoCloseIsFalse() throws Exception {
    runTestWithValidation(DisableAutoCloseInLocalVm.class);

    verify(DisableAutoCloseInLocalVm.autoCloseable, times(0)).close();
  }

  @Test
  public void accessesAutoCloseableInRemoteVm() {
    runTestWithValidation(SetAutoCloseableInRemoteVm.class);
  }

  @Test
  public void closesAutoCloseableInRemoteVm() {
    runTestWithValidation(SetAutoCloseableInRemoteVm.class);

    getVM(0).invoke(() -> verify(SetAutoCloseableInRemoteVm.autoCloseable).close());
  }

  @Test
  public void accessesAutoCloseableInEachVm() {
    runTestWithValidation(SetAutoCloseableInEachVm.class);
  }

  @Test
  public void closesAutoCloseableInEachVm() {
    runTestWithValidation(SetAutoCloseableInEachVm.class);

    for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      vm.invoke(() -> {
        verify(SetAutoCloseableInEachVm.autoCloseable).close();
      });
    }
  }

  @Test
  public void accessesCloseableInLocalVm() {
    runTestWithValidation(SetCloseableInLocalVm.class);
  }

  @Test
  public void closesCloseableInLocalVm() throws Exception {
    runTestWithValidation(SetCloseableInLocalVm.class);

    verify(SetCloseableInLocalVm.closeable).close();
  }

  @Test
  public void accessesCloseableInRemoteVm() {
    runTestWithValidation(SetCloseableInRemoteVm.class);
  }

  @Test
  public void closesCloseableInRemoteVm() {
    runTestWithValidation(SetCloseableInRemoteVm.class);

    getVM(0).invoke(() -> verify(SetCloseableInRemoteVm.closeable).close());
  }

  @Test
  public void accessesCloseableInEachVm() {
    runTestWithValidation(SetCloseableInEachVm.class);
  }

  @Test
  public void closesCloseableInEachVm() {
    runTestWithValidation(SetCloseableInEachVm.class);

    for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      vm.invoke(() -> {
        verify(SetCloseableInEachVm.closeable).close();
      });
    }
  }

  @Test
  public void accessesWithCloseInLocalVm() {
    runTestWithValidation(SetWithCloseInLocalVm.class);
  }

  @Test
  public void closesWithCloseInLocalVm() {
    runTestWithValidation(SetWithCloseInLocalVm.class);

    verify(SetWithCloseInLocalVm.withClose).close();
  }

  @Test
  public void accessesWithCloseInRemoteVm() {
    runTestWithValidation(SetWithCloseInRemoteVm.class);
  }

  @Test
  public void closesWithCloseInRemoteVm() {
    runTestWithValidation(SetWithCloseInRemoteVm.class);

    getVM(0).invoke(() -> verify(SetWithCloseInRemoteVm.withClose).close());
  }

  @Test
  public void accessesWithCloseInEachVm() {
    runTestWithValidation(SetWithCloseInEachVm.class);
  }

  @Test
  public void closesWithCloseInEachVm() {
    runTestWithValidation(SetWithCloseInEachVm.class);

    for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      vm.invoke(() -> {
        verify(SetWithCloseInEachVm.withClose).close();
      });
    }
  }

  @Test
  public void accessesWithDisconnectInLocalVm() {
    runTestWithValidation(SetWithDisconnectInLocalVm.class);
  }

  @Test
  public void disconnectsWithDisconnectInLocalVm() {
    runTestWithValidation(SetWithDisconnectInLocalVm.class);

    verify(SetWithDisconnectInLocalVm.withDisconnect).disconnect();
  }

  @Test
  public void accessesWithDisconnectInRemoteVm() {
    runTestWithValidation(SetWithDisconnectInRemoteVm.class);
  }

  @Test
  public void disconnectsWithDisconnectInRemoteVm() {
    runTestWithValidation(SetWithDisconnectInRemoteVm.class);

    getVM(0).invoke(() -> verify(SetWithDisconnectInRemoteVm.withDisconnect).disconnect());
  }

  @Test
  public void accessesWithDisconnectInEachVm() {
    runTestWithValidation(SetWithDisconnectInEachVm.class);
  }

  @Test
  public void disconnectsWithDisconnectInEachVm() {
    runTestWithValidation(SetWithDisconnectInEachVm.class);

    for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      vm.invoke(() -> {
        verify(SetWithDisconnectInEachVm.withDisconnect).disconnect();
      });
    }
  }

  public static class SetAutoCloseableInLocalVm implements Serializable {

    private static AutoCloseable autoCloseable;

    @Rule
    public DistributedReference<AutoCloseable> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      autoCloseable = mock(AutoCloseable.class);
      reference.set(autoCloseable);
    }

    @Test
    public void hasReferenceInLocalVm() {
      assertThat(reference.get()).isSameAs(autoCloseable);
    }
  }

  public static class DisableAutoCloseInLocalVm implements Serializable {

    private static AutoCloseable autoCloseable;

    @Rule
    public DistributedReference<AutoCloseable> reference =
        new DistributedReference<AutoCloseable>().autoClose(false);

    @Before
    public void setUp() {
      autoCloseable = mock(AutoCloseable.class);
      reference.set(autoCloseable);
    }

    @Test
    public void hasReferenceInLocalVm() {
      assertThat(reference.get()).isSameAs(autoCloseable);
    }
  }

  public static class SetAutoCloseableInRemoteVm implements Serializable {

    private static AutoCloseable autoCloseable;

    @Rule
    public DistributedReference<AutoCloseable> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      VM vm = getVM(0);
      vm.invoke(() -> {
        autoCloseable = mock(AutoCloseable.class, "AutoCloseable in VM-" + vm.getId());
        reference.set(autoCloseable);
      });
    }

    @Test
    public void hasAutoCloseableInRemoteVm() {
      getVM(0).invoke(() -> {
        assertThat(reference.get()).isSameAs(autoCloseable);
      });
    }
  }

  public static class SetAutoCloseableInEachVm implements Serializable {

    private static AutoCloseable autoCloseable;

    @Rule
    public DistributedReference<AutoCloseable> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          autoCloseable = mock(AutoCloseable.class, "AutoCloseable in VM-" + vm.getId());
          reference.set(autoCloseable);
        });
      }
    }

    @Test
    public void hasAutoCloseableInEachVm() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(reference.get()).isSameAs(autoCloseable);
          assertThat(reference.get().toString()).isEqualTo("AutoCloseable in VM-" + vm.getId());
        });
      }
    }
  }

  public static class SetCloseableInLocalVm implements Serializable {

    private static Closeable closeable;

    @Rule
    public DistributedReference<Closeable> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      closeable = mock(Closeable.class);
      reference.set(closeable);
    }

    @Test
    public void hasCloseableInLocalVm() {
      assertThat(reference.get()).isSameAs(closeable);
    }
  }

  public static class SetCloseableInRemoteVm implements Serializable {

    private static Closeable closeable;

    @Rule
    public DistributedReference<Closeable> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      VM vm = getVM(0);
      vm.invoke(() -> {
        closeable = mock(Closeable.class, "Closeable in VM-" + vm.getId());
        reference.set(closeable);
      });
    }

    @Test
    public void hasCloseableInRemoteVm() {
      getVM(0).invoke(() -> {
        assertThat(reference.get()).isSameAs(closeable);
      });
    }
  }

  public static class SetCloseableInEachVm implements Serializable {

    private static Closeable closeable;

    @Rule
    public DistributedReference<Closeable> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          closeable = mock(Closeable.class, "Closeable in VM-" + vm.getId());
          reference.set(closeable);
        });
      }
    }

    @Test
    public void hasCloseableInEachVm() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(reference.get()).isSameAs(closeable);
          assertThat(reference.get().toString()).isEqualTo("Closeable in VM-" + vm.getId());
        });
      }
    }
  }

  public static class SetWithCloseInLocalVm implements Serializable {

    private static WithClose withClose;

    @Rule
    public DistributedReference<WithClose> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      withClose = spy(new WithClose());
      reference.set(withClose);
    }

    @Test
    public void hasWithCloseInLocalVm() {
      assertThat(reference.get()).isSameAs(withClose);
    }
  }

  public static class SetWithCloseInRemoteVm implements Serializable {

    private static WithClose withClose;

    @Rule
    public DistributedReference<WithClose> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      VM vm = getVM(0);
      vm.invoke(() -> {
        withClose = spy(new WithClose("WithClose in VM-" + vm.getId()));
        reference.set(withClose);
      });
    }

    @Test
    public void hasWithCloseInRemoteVm() {
      getVM(0).invoke(() -> {
        assertThat(reference.get()).isSameAs(withClose);
      });
    }
  }

  public static class SetWithCloseInEachVm implements Serializable {

    private static WithClose withClose;

    @Rule
    public DistributedReference<WithClose> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          withClose = spy(new WithClose("WithClose in VM-" + vm.getId()));
          reference.set(withClose);
        });
      }
    }

    @Test
    public void hasCloseableInEachVm() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(reference.get()).isSameAs(withClose);
          assertThat(reference.get().toString()).isEqualTo("WithClose in VM-" + vm.getId());
        });
      }
    }
  }

  public static class SetWithDisconnectInLocalVm implements Serializable {

    private static WithDisconnect withDisconnect;

    @Rule
    public DistributedReference<WithDisconnect> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      withDisconnect = spy(new WithDisconnect());
      reference.set(withDisconnect);
    }

    @Test
    public void hasWithDisconnectInLocalVm() {
      assertThat(reference.get()).isSameAs(withDisconnect);
    }
  }

  public static class SetWithDisconnectInRemoteVm implements Serializable {

    private static WithDisconnect withDisconnect;

    @Rule
    public DistributedReference<WithDisconnect> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      VM vm = getVM(0);
      vm.invoke(() -> {
        withDisconnect = spy(new WithDisconnect("WithDisconnect in VM-" + vm.getId()));
        reference.set(withDisconnect);
      });
    }

    @Test
    public void hasWithDisconnectInRemoteVm() {
      getVM(0).invoke(() -> {
        assertThat(reference.get()).isSameAs(withDisconnect);
      });
    }
  }

  public static class SetWithDisconnectInEachVm implements Serializable {

    private static WithDisconnect withDisconnect;

    @Rule
    public DistributedReference<WithDisconnect> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          withDisconnect = spy(new WithDisconnect("WithDisconnect in VM-" + vm.getId()));
          reference.set(withDisconnect);
        });
      }
    }

    @Test
    public void hasWithDisconnectInEachVm() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(reference.get()).isSameAs(withDisconnect);
          assertThat(reference.get().toString()).isEqualTo("WithDisconnect in VM-" + vm.getId());
        });
      }
    }
  }

  public static class SetWithStopInLocalVm implements Serializable {

    private static WithStop withStop;

    @Rule
    public DistributedReference<WithStop> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      withStop = spy(new WithStop());
      reference.set(withStop);
    }

    @Test
    public void hasWithStopInLocalVm() {
      assertThat(reference.get()).isSameAs(withStop);
    }
  }

  public static class SetWithStopInRemoteVm implements Serializable {

    private static WithStop withStop;

    @Rule
    public DistributedReference<WithStop> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      VM vm = getVM(0);
      vm.invoke(() -> {
        withStop = spy(new WithStop("WithStop in VM-" + vm.getId()));
        reference.set(withStop);
      });
    }

    @Test
    public void hasWithStopInRemoteVm() {
      getVM(0).invoke(() -> {
        assertThat(reference.get()).isSameAs(withStop);
      });
    }
  }

  public static class SetWithStopInEachVm implements Serializable {

    private static WithStop withStop;

    @Rule
    public DistributedReference<WithStop> reference = new DistributedReference<>();

    @Before
    public void setUp() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          withStop = spy(new WithStop("WithStop in VM-" + vm.getId()));
          reference.set(withStop);
        });
      }
    }

    @Test
    public void hasWithStopInEachVm() {
      for (VM vm : toArray(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(reference.get()).isSameAs(withStop);
          assertThat(reference.get().toString()).isEqualTo("WithStop in VM-" + vm.getId());
        });
      }
    }
  }

  public static class WithClose {

    private final String value;

    WithClose() {
      this("WithClose");
    }

    WithClose(String value) {
      this.value = value;
    }

    public void close() {
      // nothing
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public static class WithDisconnect {

    private final String value;

    WithDisconnect() {
      this("WithDisconnect");
    }

    WithDisconnect(String value) {
      this.value = value;
    }

    public void disconnect() {
      // nothing
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public static class WithStop {

    private final String value;

    WithStop() {
      this("WithStop");
    }

    WithStop(String value) {
      this.value = value;
    }

    public void stop() {
      // nothing
    }

    @Override
    public String toString() {
      return value;
    }
  }
}
