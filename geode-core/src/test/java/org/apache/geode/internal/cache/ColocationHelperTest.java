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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.fake.Fakes;

public class ColocationHelperTest {
  private GemFireCacheImpl cache;
  private GemFireCacheImpl oldCacheInstance;
  private InternalDistributedSystem system;
  private PartitionedRegion pr;
  private DistributedRegion prRoot;
  private PartitionAttributes pa;
  private PartitionRegionConfig prc;
  private Logger logger;
  private Appender mockAppender;
  private ArgumentCaptor<LogEvent> loggingEventCaptor;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    system = (InternalDistributedSystem) cache.getDistributedSystem();
    pr = mock(PartitionedRegion.class);
    prRoot = mock(DistributedRegion.class);
    pa = mock(PartitionAttributes.class);
    prc = mock(PartitionRegionConfig.class);
    cache = Fakes.cache();
  }

  /**
   * Test method for
   * {@link org.apache.geode.internal.cache.ColocationHelper#getColocatedRegion(org.apache.geode.internal.cache.PartitionedRegion)}.
   */
  @Test
  public void testGetColocatedRegionThrowsIllegalStateExceptionForMissingParentRegion() {
    when(pr.getCache()).thenReturn(cache);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(mock(DistributedRegion.class));
    when(pr.getPartitionAttributes()).thenReturn(pa);
    when(pr.getFullPath()).thenReturn("/region1");
    when(pa.getColocatedWith()).thenReturn("region2");

    PartitionedRegion colocatedPR;
    boolean caughtIllegalStateException = false;
    try {
      colocatedPR = ColocationHelper.getColocatedRegion(pr);
    } catch (Exception e) {
      assertEquals("Expected IllegalStateException for missing colocated parent region",
          IllegalStateException.class, e.getClass());
      assertTrue("Expected IllegalStateException to be thrown for missing colocated region: "
          + e.getMessage(),
          e.getMessage().matches("Region specified in 'colocated-with' .* does not exist.*"));
      caughtIllegalStateException = true;
    }
    assertTrue(caughtIllegalStateException);
  }

  /**
   * Test method for
   * {@link org.apache.geode.internal.cache.ColocationHelper#getColocatedRegion(org.apache.geode.internal.cache.PartitionedRegion)}.
   */
  @Test
  public void testGetColocatedRegionLogsWarningForMissingRegionWhenPRConfigHasRegion() {
    when(pr.getCache()).thenReturn(cache);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true)).thenReturn(prRoot);
    when(pr.getPartitionAttributes()).thenReturn(pa);
    when(pr.getFullPath()).thenReturn("/region1");
    when(pa.getColocatedWith()).thenReturn("region2");
    when(((Region) prRoot).get("#region2")).thenReturn(prc);

    PartitionedRegion colocatedPR = null;
    boolean caughtIllegalStateException = false;
    try {
      colocatedPR = ColocationHelper.getColocatedRegion(pr);
    } catch (Exception e) {
      assertEquals("Expected IllegalStateException for missing colocated parent region",
          IllegalStateException.class, e.getClass());
      assertTrue("Expected IllegalStateException to be thrown for missing colocated region",
          e.getMessage().matches("Region specified in 'colocated-with' .* does not exist.*"));
      caughtIllegalStateException = true;
    }
    assertTrue(caughtIllegalStateException);
  }

  @Test
  public void testGetColocatedRegionThrowsCacheClosedExceptionWhenCacheIsClosed() {
    when(pr.getCache()).thenReturn(cache);
    DistributedRegion prRoot = mock(DistributedRegion.class);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(prRoot);
    when(pr.getPartitionAttributes()).thenReturn(pa);
    when(pa.getColocatedWith()).thenReturn("region2");
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    doThrow(CacheClosedException.class).when(cancelCriterion).checkCancelInProgress(any());

    assertThatThrownBy(() -> ColocationHelper.getColocatedRegion(pr))
        .isInstanceOf(CacheClosedException.class);
  }
}
