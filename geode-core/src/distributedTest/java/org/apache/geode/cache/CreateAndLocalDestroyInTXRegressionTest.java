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
package org.apache.geode.cache;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PROXY;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region.Entry;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Create and LocalDestroy/LocalInvalidate should create event with NewValue
 *
 * <p>
 * TRAC #34387: TX in Proxy Regions with create followed by localDestroy on same key results in
 * remote VMs receiving create events with null getNewValue().
 *
 * @since GemFire 5.0
 */

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class CreateAndLocalDestroyInTXRegressionTest implements Serializable {

  private static final String REGION_NAME = "r1";
  private static final String KEY = "createKey";
  private static final String VALUE = "createValue";

  private transient CacheListener<String, String> spyCacheListener;

  private VM otherVM;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    otherVM = getVM(0);
    spyCacheListener = mock(CacheListener.class);

    otherVM.invoke(() -> {
      RegionFactory<String, String> regionFactory =
          cacheRule.getOrCreateCache().createRegionFactory(REPLICATE_PROXY);
      regionFactory.create(REGION_NAME);
    });

    RegionFactory<String, String> regionFactory =
        cacheRule.getOrCreateCache().createRegionFactory(REPLICATE);
    regionFactory.addCacheListener(spyCacheListener);
    regionFactory.create(REGION_NAME);
  }

  @Test
  @Parameters({"LOCAL_DESTROY", "LOCAL_INVALIDATE"})
  @TestCaseName("{method}({params})")
  public void createAndLocalOpShouldCreateEventWithNewValue(final LocalOperation operation) {
    otherVM.invoke(() -> {
      CacheTransactionManager transactionManager =
          cacheRule.getCache().getCacheTransactionManager();
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);

      transactionManager.begin();
      try {
        region.create(KEY, VALUE);

        assertThatThrownBy(() -> operation.invoke(region))
            .isInstanceOf(UnsupportedOperationInTransactionException.class)
            .hasMessage(operation.getMessage());
      } finally {
        transactionManager.commit();
      }
    });

    Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
    Entry<String, String> entry = region.getEntry(KEY);

    assertThat(entry.getValue()).isEqualTo(VALUE);

    ArgumentCaptor<EntryEvent<String, String>> argument = ArgumentCaptor.forClass(EntryEvent.class);
    verify(spyCacheListener, times(1)).afterCreate(argument.capture());

    EntryEvent<String, String> event = argument.getValue();
    assertThat(event.getKey()).isEqualTo(KEY);
    assertThat(event.getNewValue()).isEqualTo(VALUE);
  }

  private enum LocalOperation {
    LOCAL_DESTROY((region) -> region.localDestroy(KEY),
        "localDestroy() is not allowed in a transaction"),
    LOCAL_INVALIDATE((region) -> region.localInvalidate(KEY),
        "localInvalidate() is not allowed in a transaction");

    private final Consumer<Region<String, String>> strategy;
    private final String message;

    LocalOperation(final Consumer<Region<String, String>> strategy, final String message) {
      this.strategy = strategy;
      this.message = message;
    }

    void invoke(final Region<String, String> region) {
      strategy.accept(region);
    }

    String getMessage() {
      return message;
    }
  }
}
