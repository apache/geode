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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class CacheRuleTest {

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Test
  public void defaultDoesNothing() throws Exception {
    runTestWithValidation(DefaultDoesNothing.class);
  }

  @Test
  public void createCacheInLocal() throws Exception {
    runTestWithValidation(CreateCacheInLocal.class);
  }

  @Test
  public void createCacheInOneVM() throws Exception {
    runTestWithValidation(CreateCacheInOneVM.class);
  }

  @Test
  public void createCacheInTwoVMs() throws Exception {
    runTestWithValidation(CreateCacheInTwoVMs.class);
  }

  @Test
  public void createCacheInAll() throws Exception {
    runTestWithValidation(CreateCacheInAll.class);
  }

  @Test
  public void createCacheInAllExplicitly() throws Exception {
    runTestWithValidation(CreateCacheInAllExplicitly.class);
  }

  @Test
  public void createCacheInAllCreatesCluster() throws Exception {
    runTestWithValidation(CreateCacheInAllCreatesCluster.class);
  }

  @Test
  public void addConfigAffectsAllVMs() throws Exception {
    runTestWithValidation(AddConfigAffectsAllVMs.class);
  }

  @Test
  public void replaceConfigCreatesLonersInAll() throws Exception {
    runTestWithValidation(ReplaceConfigCreatesLonersInAll.class);
  }

  /**
   * Used by test {@link #defaultDoesNothing()}.
   */
  public static class DefaultDoesNothing implements Serializable {

    @Rule
    public CacheRule cacheRule = CacheRule.builder().build();

    @Test
    public void getCache_returnsNullInAllVMs() throws Exception {
      assertThat(cacheRule.getCache()).isNull();
      for (VM vm : Host.getHost(0).getAllVMs()) {
        vm.invoke(() -> assertThat(cacheRule.getCache()).isNull());
      }
    }

    @Test
    public void getCacheSingleton_returnsNullInAllVMs() throws Exception {
      assertThat(GemFireCacheImpl.getInstance()).isNull();
      for (VM vm : Host.getHost(0).getAllVMs()) {
        vm.invoke(() -> assertThat(GemFireCacheImpl.getInstance()).isNull());
      }
    }
  }

  /**
   * Used by test {@link #createCacheInLocal()}.
   */
  public static class CreateCacheInLocal implements Serializable {

    @Rule
    public CacheRule cacheRule = CacheRule.builder().createCacheInLocal().build();

    @Test
    public void getCache_returnsCacheInLocalOnly() throws Exception {
      assertThat(cacheRule.getCache()).isNotNull();
      for (VM vm : Host.getHost(0).getAllVMs()) {
        vm.invoke(() -> assertThat(cacheRule.getCache()).isNull());
      }
    }
  }

  /**
   * Used by test {@link #createCacheInOneVM()}.
   */
  public static class CreateCacheInOneVM implements Serializable {

    @Rule
    public CacheRule cacheRule =
        CacheRule.builder().createCacheIn(Host.getHost(0).getVM(0)).build();

    @Test
    public void getCache_returnsCacheInOneVM() throws Exception {
      assertThat(cacheRule.getCache()).isNull();

      Host.getHost(0).getVM(0).invoke(() -> {
        assertThat(cacheRule.getCache()).isNotNull();
      });

      for (int i = 1; i < Host.getHost(0).getVMCount(); i++) {
        Host.getHost(0).getVM(i).invoke(() -> {
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
    public CacheRule cacheRule = CacheRule.builder().createCacheIn(Host.getHost(0).getVM(1))
        .createCacheIn(Host.getHost(0).getVM(3)).build();

    @Test
    public void getCache_returnsCacheInTwoVMs() throws Exception {
      assertThat(cacheRule.getCache()).isNull();

      for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
        if (i == 1 || i == 3) {
          Host.getHost(0).getVM(i).invoke(() -> {
            assertThat(cacheRule.getCache()).isNotNull();
          });
        } else {
          Host.getHost(0).getVM(i).invoke(() -> {
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
    public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

    @Test
    public void createCacheInAll_returnsCacheInAll() throws Exception {
      assertThat(cacheRule.getCache()).isNotNull();
      for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
        Host.getHost(0).getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache()).isNotNull();
        });
      }
    }
  }

  /**
   * Used by test {@link #createCacheInAllExplicitly()}.
   */
  public static class CreateCacheInAllExplicitly implements Serializable {

    @Rule
    public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

    @Test
    public void createCacheInAllExplicitly_returnsCacheInAll() throws Exception {
      assertThat(cacheRule.getCache()).isNotNull();
      for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
        Host.getHost(0).getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache()).isNotNull();
        });
      }
    }
  }

  /**
   * Used by test {@link #createCacheInAllCreatesCluster()}.
   */
  public static class CreateCacheInAllCreatesCluster implements Serializable {

    @Rule
    public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

    @Test
    public void createCacheInAll_createsCluster() throws Exception {
      int vmCount = Host.getHost(0).getVMCount();

      assertThat(cacheRule.getCache().getDistributionManager().getViewMembers())
          .hasSize(vmCount + 2);
      for (int i = 0; i < vmCount; i++) {
        Host.getHost(0).getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache().getDistributionManager().getViewMembers())
              .hasSize(vmCount + 2);
        });
      }
    }
  }

  /**
   * Used by test {@link #addConfigAffectsAllVMs()}.
   */
  public static class AddConfigAffectsAllVMs implements Serializable {

    @Rule
    public CacheRule cacheRule =
        CacheRule.builder().addConfig(DISTRIBUTED_SYSTEM_ID, "1").createCacheInAll().build();

    @Test
    public void addConfig_affectsAllVMs() throws Exception {
      assertThat(cacheRule.getCache().getDistributedSystem().getProperties()
          .getProperty(DISTRIBUTED_SYSTEM_ID)).isEqualTo("1");
      assertThat(
          cacheRule.getCache().getInternalDistributedSystem().getConfig().getDistributedSystemId())
              .isEqualTo(1);
      for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
        Host.getHost(0).getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache().getDistributedSystem().getProperties()
              .getProperty(DISTRIBUTED_SYSTEM_ID)).isEqualTo("1");
        });
        Host.getHost(0).getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache().getInternalDistributedSystem().getConfig()
              .getDistributedSystemId()).isEqualTo(1);
        });
      }
    }
  }

  /**
   * Used by test {@link #replaceConfigCreatesLonersInAll()}.
   */
  public static class ReplaceConfigCreatesLonersInAll implements Serializable {

    @Rule
    public CacheRule cacheRule =
        CacheRule.builder().replaceConfig(new Properties()).createCacheInAll().build();

    @Test
    public void replaceConfig_createsLonersInAll() throws Exception {
      assertThat(cacheRule.getCache().getDistributionManager().getViewMembers()).hasSize(1);
      for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
        Host.getHost(0).getVM(i).invoke(() -> {
          assertThat(cacheRule.getCache().getDistributionManager().getViewMembers()).hasSize(1);
        });
      }
    }
  }
}
