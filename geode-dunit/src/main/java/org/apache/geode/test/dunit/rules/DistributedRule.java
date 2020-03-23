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

import static org.apache.geode.test.dunit.Disconnect.disconnectFromDS;
import static org.apache.geode.test.dunit.DistributedTestUtils.unregisterInstantiatorsInThisVM;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.Invoke.invokeInLocator;
import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;

import java.util.Properties;

import org.apache.geode.cache.query.QueryTestUtils;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.cache30.RegionTestCase;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.ClientStatsManager;
import org.apache.geode.internal.cache.DiskStoreObserver;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * JUnit Rule that launches DistributedTest VMs and scans all log output for suspect strings without
 * {@code DistributedTestCase}. The test class may need to implement {@code Serializable} if it
 * uses lambdas to perform {@code RMI} invocations on {@code VM}s.
 *
 * <p>
 * {@code DistributedRule} can be used in DistributedTests as a {@code Rule}. This will ensure
 * that checking for suspect strings is performed after each test method.
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Test
 * public void shouldHaveFourVMsByDefault() {
 *   assertThat(getVMCount()).isEqualTo(4);
 * }
 * </pre>
 *
 * <p>
 * You may specify a non-default number of {@code VM}s for the test when constructing
 * {@code DistributedRule}.
 *
 * <p>
 * Example of specifying fewer that the default number of {@code VM}s (which is 4):
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule(1);
 *
 * {@literal @}Test
 * public void hasOneVM() {
 *   assertThat(getVMCount()).isEqualTo(1);
 * }
 * </pre>
 *
 * <p>
 * Example of specifying greater that the default number of {@code VM}s (which is 4):
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule(8);
 *
 * {@literal @}Test
 * public void hasEightVMs() {
 *   assertThat(getVMCount()).isEqualTo(8);
 * }
 * </pre>
 *
 * <p>
 * {@code DistributedRule} can also be used in DistributedTests as a {@code ClassRule}. This ensures
 * that DUnit VMs will be available to non-Class {@code Rule}s. However, you may want to declare
 * {@code DistributedRule.TearDown} as a non-Class {@code Rule} so that check for suspect strings is
 * performed after each test method.
 *
 * <pre>
 * {@literal @}ClassRule
 * public static DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public DistributedRule.TearDown distributedRuleTearDown = new DistributedRule.TearDown();
 *
 * {@literal @}Test
 * public void shouldHaveFourDUnitVMsByDefault() {
 *   assertThat(getVMCount()).isEqualTo(4);
 * }
 * </pre>
 */
@SuppressWarnings("unused")
public class DistributedRule extends AbstractDistributedRule {

  /**
   * Use {@code Builder} for more options in constructing {@code DistributedRule}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Constructs DistributedRule and launches the default number of {@code VM}s (which is 4).
   */
  public DistributedRule() {
    this(new Builder());
  }

  /**
   * Constructs DistributedRule and launches the specified number of {@code VM}s.
   *
   * @param vmCount specified number of VMs
   */
  public DistributedRule(final int vmCount) {
    this(new Builder().withVMCount(vmCount));
  }

  private DistributedRule(final Builder builder) {
    super(builder.vmCount);
  }

  @Override
  protected void after() {
    TearDown.doTearDown();
  }

  public static Properties getDistributedSystemProperties() {
    return DUnitLauncher.getDistributedSystemProperties();
  }

  /**
   * Builds an instance of DistributedRule.
   */
  public static class Builder {

    private int vmCount = DEFAULT_VM_COUNT;

    public Builder withVMCount(final int vmCount) {
      if (vmCount < 0) {
        throw new IllegalArgumentException("VM count must be positive integer");
      }
      this.vmCount = vmCount;
      return this;
    }

    public DistributedRule build() {
      return new DistributedRule(this);
    }
  }

  /**
   * Cleans up horrendous things like static state and non-default instances in Geode.
   *
   * <p>
   * {@link DistributedRule#after()} invokes the same cleanup that this Rule does, but if you
   * defined {@code DistributedRule} as a {@code ClassRule} then you should declare TearDown
   * as a non-class {@code Rule} in your test:
   *
   * <pre>
   * {@literal @}ClassRule
   * public static DistributedRule distributedRule = new DistributedRule();
   *
   * {@literal @}Rule
   * public DistributedRule.TearDown tearDownRule = new DistributedRule.TearDown();
   *
   * {@literal @}Test
   * public void shouldHaveFourDUnitVMsByDefault() {
   *   assertThat(getVMCount()).isEqualTo(4);
   * }
   * </pre>
   *
   * <p>
   * Note: {@link CacheRule} handles its own cleanup of Cache and Regions.
   */
  public static class TearDown extends SerializableExternalResource {

    @Override
    protected void before() {
      // nothing
    }

    @Override
    protected void after() {
      doTearDown();
    }

    static void doTearDown() {
      tearDownInVM();
      invokeInEveryVM(() -> {
        tearDownInVM();
      });
      invokeInLocator(() -> {
        DistributionMessageObserver.setInstance(null);
        unregisterInstantiatorsInThisVM();
      });
      DUnitLauncher.closeAndCheckForSuspects();
    }

    public static void tearDownInVM() {
      // 1. Please do NOT add to this list. I'm trying to DELETE this list.
      // 2. Instead, please add to the after() of your test or your rule.

      disconnectFromDS();
      // keep alphabetized to detect duplicate lines
      CacheCreation.clearThreadLocals();
      CacheServerTestUtil.clearCacheReference();
      ClientProxyMembershipID.system = null;
      ClientServerTestCase.AUTO_LOAD_BALANCE = false;
      ClientStatsManager.cleanupForTests();
      DiskStoreObserver.setInstance(null);
      unregisterInstantiatorsInThisVM();
      DistributionMessageObserver.setInstance(null);
      InitialImageOperation.slowImageProcessing = 0;
      InternalClientMembership.unregisterAllListeners();
      LogWrapper.close();
      QueryObserverHolder.reset();
      QueryTestUtils.setCache(null);
      RegionTestCase.preSnapshotRegion = null;
      SocketCreator.resetHostNameCache();
      SocketCreator.resolve_dns = true;

      // clear system properties -- keep alphabetized
      System.clearProperty(GeodeGlossary.GEMFIRE_PREFIX + "log-level");
      System.clearProperty("jgroups.resolve_dns");
      System.clearProperty(Message.MAX_MESSAGE_SIZE_PROPERTY);

      if (InternalDistributedSystem.systemAttemptingReconnect != null) {
        InternalDistributedSystem.systemAttemptingReconnect.stopReconnecting();
      }

      IgnoredException.removeAllExpectedExceptions();
      SocketCreatorFactory.close();
      TypeRegistry.setPdxSerializer(null);
      TypeRegistry.init();
    }
  }
}
