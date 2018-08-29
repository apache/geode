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

import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class DistributedRestoreSystemPropertiesDistributedTest {

  private static final String NULL_PROPERTY = "NULL_PROPERTY";
  private static final String PREEXISTING_PROPERTY = "PREEXISTING_PROPERTY";
  private static final String PREEXISTING_VALUE = "PREEXISTING_VALUE";

  @ClassRule
  public static DistributedRule distributedRule = new DistributedRule();

  @BeforeClass
  public static void assertPreconditions() {
    assertThat(System.getProperty(NULL_PROPERTY)).isNull();
    assertThat(System.getProperty(PREEXISTING_PROPERTY)).isNull();
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        assertThat(System.getProperty(NULL_PROPERTY)).isNull();
        assertThat(System.getProperty(PREEXISTING_PROPERTY)).isNull();
      });
    }
  }

  @Before
  public void setUp() {
    System.setProperty(PREEXISTING_PROPERTY, PREEXISTING_VALUE);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        System.setProperty(PREEXISTING_PROPERTY, PREEXISTING_VALUE);
      });
    }
  }

  @After
  public void tearDown() {
    System.clearProperty(PREEXISTING_PROPERTY);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        System.clearProperty(PREEXISTING_PROPERTY);
      });
    }
  }

  @Test
  public void nullPropertyWithDifferentValues() {
    runTestWithValidation(NullPropertyWithDifferentValues.class);

    assertThat(System.getProperty(NULL_PROPERTY)).isNull();
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        assertThat(System.getProperty(NULL_PROPERTY)).isNull();
      });
    }
  }

  @Test
  public void preexistingPropertyWithDifferentValues() {
    runTestWithValidation(NullPropertyWithDifferentValues.class);

    assertThat(System.getProperty(PREEXISTING_PROPERTY)).isEqualTo(PREEXISTING_VALUE);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        assertThat(System.getProperty(PREEXISTING_PROPERTY)).isEqualTo(PREEXISTING_VALUE);
      });
    }
  }

  /**
   * Used by test {@link #nullPropertyWithDifferentValues()}.
   */
  public static class NullPropertyWithDifferentValues implements Serializable {

    @Rule
    public DistributedRestoreSystemProperties restoreSystemProperties =
        new DistributedRestoreSystemProperties();

    @Test
    public void nullPropertyWithDifferentValues() {
      System.setProperty(NULL_PROPERTY, "controller");
      getVM(0).invoke(() -> System.setProperty(NULL_PROPERTY, "vm0"));
      getVM(1).invoke(() -> System.setProperty(NULL_PROPERTY, "vm1"));
      getVM(2).invoke(() -> System.setProperty(NULL_PROPERTY, "vm2"));
      getVM(3).invoke(() -> System.setProperty(NULL_PROPERTY, "vm3"));
    }
  }

  /**
   * Used by test {@link #preexistingPropertyWithDifferentValues()}.
   */
  public static class PreexistingPropertyWithDifferentValues implements Serializable {

    @Rule
    public DistributedRestoreSystemProperties restoreSystemProperties =
        new DistributedRestoreSystemProperties();

    @Test
    public void preexistingPropertyWithDifferentValues() {
      System.setProperty(PREEXISTING_PROPERTY, "controller");
      getVM(0).invoke(() -> System.setProperty(PREEXISTING_PROPERTY, "vm0"));
      getVM(1).invoke(() -> System.setProperty(PREEXISTING_PROPERTY, "vm1"));
      getVM(2).invoke(() -> System.setProperty(PREEXISTING_PROPERTY, "vm2"));
      getVM(3).invoke(() -> System.setProperty(PREEXISTING_PROPERTY, "vm3"));
    }
  }
}
