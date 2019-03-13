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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;

public class PRHARedundancyProviderTest {
  private PRHARedundancyProvider provider;
  private final int numberOfBuckets = 113;
  private CacheClosedException expected;

  @Before
  public void setup() {
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    provider = spy(new PRHARedundancyProvider(partitionedRegion));
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsIfStartRedundancyLoggerThrows()
      throws Exception {
    provider.allBucketsRecoveredFromDisk = new CountDownLatch(numberOfBuckets);
    doThrow(new CacheClosedException()).when(provider).createRedundancyLogger();

    Thread thread = new Thread(() -> startRedundancyLogger());
    thread.start();

    provider.waitForPersistentBucketRecovery();
    thread.join();

    assertThat(expected).isNotNull();
  }

  private void startRedundancyLogger() {
    try {
      provider.startRedundancyLogger(numberOfBuckets);
    } catch (CacheClosedException e) {
      expected = e;
    }
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsWhenAllBucketsRecoveredFromDiskLatchIsNull() {
    provider.waitForPersistentBucketRecovery();
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsAfterLatchCountDown() throws Exception {
    provider.allBucketsRecoveredFromDisk = spy(new CountDownLatch(1));
    provider.allBucketsRecoveredFromDisk.countDown();

    provider.waitForPersistentBucketRecovery();

    verify(provider.allBucketsRecoveredFromDisk).await();
  }
}
