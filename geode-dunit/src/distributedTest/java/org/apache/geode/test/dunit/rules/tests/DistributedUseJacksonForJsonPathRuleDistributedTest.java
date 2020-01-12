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

import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.DistributedUseJacksonForJsonPathRule;

/**
 * Distributed tests for {@link DistributedUseJacksonForJsonPathRule}.
 */
@SuppressWarnings("serial")
public class DistributedUseJacksonForJsonPathRuleDistributedTest implements Serializable {

  private static final Map<VM, String> jsonProviderMap = new ConcurrentHashMap<>();
  private static final Map<VM, String> mappingProviderMap = new ConcurrentHashMap<>();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  private String defaultJsonProvider;
  private String defaultMappingProvider;
  private int initialVmCount;

  @Before
  public void setUp() {
    Configuration config = Configuration.defaultConfiguration();
    defaultJsonProvider = config.jsonProvider().getClass().getName();
    defaultMappingProvider = config.mappingProvider().getClass().getName();
    initialVmCount = getVMCount();
  }

  @After
  public void tearDown() {
    jsonProviderMap.clear();
    mappingProviderMap.clear();
  }

  @Test
  public void setsJsonProviderToJacksonJsonProviderBeforeTest() {
    runTestWithValidation(CaptureJsonProvider.class);

    for (VM vm : toArray(getAllVMs(), getController())) {
      assertThat(jsonProviderMap.get(vm)).isEqualTo(JacksonJsonProvider.class.getName());
    }
  }

  @Test
  public void setsMappingProviderToJacksonMappingProviderBeforeTest() {
    runTestWithValidation(CaptureMappingProvider.class);

    for (VM vm : toArray(getAllVMs(), getController())) {
      assertThat(mappingProviderMap.get(vm)).isEqualTo(JacksonMappingProvider.class.getName());
    }
  }

  @Test
  public void restoresJsonProviderToDefaultAfterTest() {
    runTestWithValidation(HasUseJacksonForJsonPathRule.class);

    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        Configuration config = Configuration.defaultConfiguration();
        assertThat(config.jsonProvider().getClass().getName()).isEqualTo(defaultJsonProvider);
      });
    }
  }

  @Test
  public void restoresMappingProviderToDefaultAfterTest() {
    runTestWithValidation(HasUseJacksonForJsonPathRule.class);

    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        Configuration config = Configuration.defaultConfiguration();
        assertThat(config.mappingProvider().getClass().getName()).isEqualTo(defaultMappingProvider);
      });
    }
  }

  @Test
  public void setsJsonProviderToJacksonJsonProviderBeforeTest_inNewVm() {
    runTestWithValidation(CaptureJsonProviderInNewVm.class);

    VM newVM = getVM(initialVmCount);
    assertThat(jsonProviderMap.get(newVM)).isEqualTo(JacksonJsonProvider.class.getName());
  }

  @Test
  public void setsMappingProviderToJacksonMappingProviderBeforeTest_inNewVm() {
    runTestWithValidation(CaptureMappingProviderInNewVm.class);

    VM newVM = getVM(initialVmCount);
    assertThat(mappingProviderMap.get(newVM)).isEqualTo(JacksonMappingProvider.class.getName());
  }

  @Test
  public void restoresJsonProviderToDefaultAfterTest_inNewVm() {
    runTestWithValidation(UseJacksonForJsonPathRuleWithNewVm.class);

    VM newVM = getVM(initialVmCount);
    newVM.invoke(() -> {
      Configuration config = Configuration.defaultConfiguration();
      assertThat(config.jsonProvider().getClass().getName()).isEqualTo(defaultJsonProvider);
    });
  }

  @Test
  public void restoresMappingProviderToDefaultAfterTest_inNewVm() {
    runTestWithValidation(UseJacksonForJsonPathRuleWithNewVm.class);

    VM newVM = getVM(initialVmCount);
    newVM.invoke(() -> {
      Configuration config = Configuration.defaultConfiguration();
      assertThat(config.mappingProvider().getClass().getName()).isEqualTo(defaultMappingProvider);
    });
  }

  @Test
  public void setsJsonProviderToJacksonJsonProviderBeforeTest_inBouncedVm() {
    runTestWithValidation(CaptureJsonProviderInBouncedVm.class);

    VM bouncedVM = getVM(0);
    assertThat(jsonProviderMap.get(bouncedVM)).isEqualTo(JacksonJsonProvider.class.getName());
  }

  @Test
  public void setsMappingProviderToJacksonMappingProviderBeforeTest_inBouncedVm() {
    runTestWithValidation(CaptureMappingProviderInBouncedVm.class);

    VM bouncedVM = getVM(0);
    assertThat(mappingProviderMap.get(bouncedVM)).isEqualTo(JacksonMappingProvider.class.getName());
  }

  @Test
  public void restoresJsonProviderToDefaultAfterTest_inBouncedVm() {
    runTestWithValidation(UseJacksonForJsonPathRuleWithBouncedVm.class);

    VM bouncedVM = getVM(0);
    bouncedVM.invoke(() -> {
      Configuration config = Configuration.defaultConfiguration();
      assertThat(config.jsonProvider().getClass().getName()).isEqualTo(defaultJsonProvider);
    });
  }

  @Test
  public void restoresMappingProviderToDefaultAfterTest_inBouncedVm() {
    runTestWithValidation(UseJacksonForJsonPathRuleWithBouncedVm.class);

    VM bouncedVM = getVM(0);
    bouncedVM.invoke(() -> {
      Configuration config = Configuration.defaultConfiguration();
      assertThat(config.mappingProvider().getClass().getName()).isEqualTo(defaultMappingProvider);
    });
  }

  public static class HasUseJacksonForJsonPathRule implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void doNothing() {
      assertThat(useJacksonForJsonPathRule).isNotNull();
    }
  }

  public static class CaptureJsonProvider implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void captureJsonProvider() {
      for (VM vm : toArray(getAllVMs(), getController())) {
        Class<? extends JsonProvider> jsonProviderClass = vm.invoke(() -> {
          Configuration config = Configuration.defaultConfiguration();
          return config.jsonProvider().getClass();
        });
        assertThat(jsonProviderClass).isNotNull();
        jsonProviderMap.put(vm, jsonProviderClass.getName());
      }
    }
  }

  public static class CaptureMappingProvider implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void captureMappingProvider() {
      for (VM vm : toArray(getAllVMs(), getController())) {
        Class<? extends MappingProvider> mappingProviderClass = vm.invoke(() -> {
          Configuration config = Configuration.defaultConfiguration();
          return config.mappingProvider().getClass();
        });
        assertThat(mappingProviderClass).isNotNull();
        mappingProviderMap.put(vm, mappingProviderClass.getName());
      }
    }
  }

  public static class CaptureJsonProviderInNewVm implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void captureJsonProviderInNewVm() {
      VM newVM = getVM(getVMCount());
      Class<? extends JsonProvider> jsonProviderClass = newVM.invoke(() -> {
        Configuration config = Configuration.defaultConfiguration();
        return config.jsonProvider().getClass();
      });
      assertThat(jsonProviderClass).isNotNull();
      jsonProviderMap.put(newVM, jsonProviderClass.getName());
    }
  }

  public static class CaptureMappingProviderInNewVm implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void captureMappingProviderInNewVm() {
      VM newVM = getVM(getVMCount());
      Class<? extends MappingProvider> mappingProviderClass = newVM.invoke(() -> {
        Configuration config = Configuration.defaultConfiguration();
        return config.mappingProvider().getClass();
      });
      assertThat(mappingProviderClass).isNotNull();
      mappingProviderMap.put(newVM, mappingProviderClass.getName());
    }
  }

  public static class UseJacksonForJsonPathRuleWithNewVm implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void getNewVm() {
      getVM(getVMCount());
    }
  }

  public static class CaptureJsonProviderInBouncedVm implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void captureJsonProviderInBouncedVm() {
      VM vm = getVM(0);
      vm.bounce();
      Class<? extends JsonProvider> jsonProviderClass = vm.invoke(() -> {
        Configuration config = Configuration.defaultConfiguration();
        return config.jsonProvider().getClass();
      });
      assertThat(jsonProviderClass).isNotNull();
      jsonProviderMap.put(vm, jsonProviderClass.getName());
    }
  }

  public static class CaptureMappingProviderInBouncedVm implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void captureMappingProviderInBouncedVm() {
      VM vm = getVM(0);
      vm.bounce();
      Class<? extends MappingProvider> mappingProviderClass = vm.invoke(() -> {
        Configuration config = Configuration.defaultConfiguration();
        return config.mappingProvider().getClass();
      });
      assertThat(mappingProviderClass).isNotNull();
      mappingProviderMap.put(vm, mappingProviderClass.getName());
    }
  }

  public static class UseJacksonForJsonPathRuleWithBouncedVm implements Serializable {

    @Rule
    public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
        new DistributedUseJacksonForJsonPathRule();

    @Test
    public void getNewVm() {
      VM vm = getVM(0);
      vm.bounce();
    }
  }
}
