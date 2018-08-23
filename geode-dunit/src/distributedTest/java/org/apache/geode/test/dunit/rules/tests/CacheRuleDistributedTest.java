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
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class CacheRuleDistributedTest {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Test
  public void defaultDoesNothing() {
    runTestWithValidation(DefaultDoesNothing.class);
  }

  @Test
  public void createCacheInLocal() {
    runTestWithValidation(CreateCacheInLocal.class);
  }

  @Test
  public void createCacheInOneVM() {
    runTestWithValidation(CreateCacheInOneVM.class);
  }

  @Test
  public void createCacheInTwoVMs() {
    runTestWithValidation(CreateCacheInTwoVMs.class);
  }

  @Test
  public void createCacheInAll() {
    runTestWithValidation(CreateCacheInAll.class);
  }

  @Test
  public void createCacheInAllCreatesCluster() {
    runTestWithValidation(CreateCacheInAllCreatesCluster.class);
  }

  @Test
  public void emptyConfigCreatesLonersInAll() {
    runTestWithValidation(EmptyConfigCreatesLonersInAll.class);
  }

  /**
   * Used by test {@link #defaultDoesNothing()}.
   */
  public static class DefaultDoesNothing implements Serializable {

    @Rule
    public CacheRule cacheRule = new CacheRule();

    @Test
    public void getCache_returnsNullInAllVMs() {
      assertThat(cacheRule.getCache()).isNull();
      for (VM vm : getAllVMs()) {
        vm.invoke(() -> assertThat(cacheRule.getCache()).isNull());
      }
    }

    @Test
    public void getCacheSingleton_returnsNullInAllVMs() {
      assertThat(GemFireCacheImpl.getInstance()).isNull();
      for (VM vm : getAllVMs()) {
        vm.invoke(() -> assertThat(GemFireCacheImpl.getInstance()).isNull());
      }
    }
  }

  /**
   * Used by test {@link #createCacheInLocal()}.
   */
  public static class CreateCacheInLocal implements Serializable {

    @Rule
    public CacheRule cacheRule = new CacheRule();

    @Before
    public void setUp() throws Exception {
      cacheRule.createCache();
    }

    @Test
    public void getCache_returnsCacheInLocalOnly() {
      assertThat(cacheRule.getCache()).isNotNull();
      for (VM vm : getAllVMs()) {
        vm.invoke(() -> assertThat(cacheRule.getCache()).isNull());
      }
    }
  }

  /**
   * Used by test {@link #createCacheInOneVM()}.
   */
  public static class CreateCacheInOneVM implements Serializable {

    @Rule
    public CacheRule cacheRule = new CacheRule();

    @Before
    public void setUp() throws Exception {
      getVM(0).invoke(() -> {
        cacheRule.createCache();
      });
    }

    @Test
    public void getCache_returnsCacheInOneVM() {
      assertThat(cacheRule.getCache()).isNull();

      getVM(0).invoke(() -> {
        assertThat(cacheRule.getCache()).isNotNull();
      });

      for (int i = 1; i < getVMCount(); i++) {
        getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache()).isNull();
        });
      }
    }
  }

  /**
   * Used by test {@link #createCacheInTwoVMs()}.
   */
  public static class CreateCacheInTwoVMs implements Serializable {

    @Rule
    public CacheRule cacheRule = new CacheRule();

    @Before
    public void setUp() throws Exception {
      getVM(1).invoke(() -> {
        cacheRule.createCache();
      });
      getVM(3).invoke(() -> {
        cacheRule.createCache();
      });
    }

    @Test
    public void getCache_returnsCacheInTwoVMs() {
      assertThat(cacheRule.getCache()).isNull();

      for (int i = 0; i < getVMCount(); i++) {
        if (i == 1 || i == 3) {
          getVM(i).invoke(() -> {
            assertThat(cacheRule.getCache()).isNotNull();
          });
        } else {
          getVM(i).invoke(() -> {
            assertThat(cacheRule.getCache()).isNull();
          });
        }
      }
    }
  }

  /**
   * Used by test {@link #createCacheInAll()}.
   */
  public static class CreateCacheInAll implements Serializable {

    @Rule
    public CacheRule cacheRule = new CacheRule();

    @Before
    public void setUp() throws Exception {
      cacheRule.createCache();
      for (int i = 0; i < getVMCount(); i++) {
        getVM(i).invoke(() -> {
          cacheRule.createCache();
        });
      }
    }

    @Test
    public void createCacheInAll_returnsCacheInAll() {
      assertThat(cacheRule.getCache()).isNotNull();
      for (int i = 0; i < getVMCount(); i++) {
        getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache()).isNotNull();
        });
      }
    }
  }

  /**
   * Used by test {@link #createCacheInAllCreatesCluster()}.
   */
  public static class CreateCacheInAllCreatesCluster implements Serializable {

    private int vmCount;

    @Rule
    public CacheRule cacheRule = new CacheRule();

    @Before
    public void setUp() throws Exception {
      vmCount = getVMCount();

      cacheRule.createCache();
      for (int i = 0; i < vmCount; i++) {
        getVM(i).invoke(() -> {
          cacheRule.createCache();
        });
      }
    }

    @Test
    public void createCacheInAll_createsCluster() {
      assertThat(cacheRule.getCache().getDistributionManager().getViewMembers())
          .hasSize(vmCount + 2);
      for (int i = 0; i < vmCount; i++) {
        getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache().getDistributionManager().getViewMembers())
              .hasSize(vmCount + 2);
        });
      }
    }
  }

  /**
   * Used by test {@link #emptyConfigCreatesLonersInAll()}.
   */
  public static class EmptyConfigCreatesLonersInAll implements Serializable {

    private Properties config;

    @Rule
    public CacheRule cacheRule = CacheRule.builder().replaceConfig(new Properties()).build();

    @Before
    public void setUp() throws Exception {
      config = new Properties();

      cacheRule.createCache(config);
      for (int i = 0; i < getVMCount(); i++) {
        getVM(i).invoke(() -> cacheRule.createCache(config));
      }
    }

    @Test
    public void emptyConfig_createsLonersInAll() {
      assertThat(cacheRule.getCache().getDistributionManager().getViewMembers()).hasSize(1);
      for (int i = 0; i < getVMCount(); i++) {
        getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache().getDistributionManager().getViewMembers()).hasSize(1);
        });
      }
    }
  }
}
