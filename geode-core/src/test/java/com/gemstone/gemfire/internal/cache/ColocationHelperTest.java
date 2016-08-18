/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.ArgumentCaptor;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;

import org.apache.logging.log4j.core.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
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
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

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
    oldCacheInstance = GemFireCacheImpl.setInstanceForTests(cache);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    GemFireCacheImpl.setInstanceForTests(oldCacheInstance);
  }

  /**
   * Test method for {@link com.gemstone.gemfire.internal.cache.ColocationHelper#getColocatedRegion(com.gemstone.gemfire.internal.cache.PartitionedRegion)}.
   */
  @Test
  public void testGetColocatedRegionThrowsIllegalStateExceptionForMissingParentRegion() {
    when(pr.getCache()).thenReturn(cache);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true)).thenReturn(mock(DistributedRegion.class));
    when(pr.getPartitionAttributes()).thenReturn(pa);
    when(pr.getFullPath()).thenReturn("/region1");
    when(pa.getColocatedWith()).thenReturn("region2");

    PartitionedRegion colocatedPR;
    boolean caughtIllegalStateException = false;
    try {
      colocatedPR = ColocationHelper.getColocatedRegion(pr);
    } catch (Exception e) {
      assertEquals("Expected IllegalStateException for missing colocated parent region", IllegalStateException.class, e.getClass());
      assertTrue("Expected IllegalStateException to be thrown for missing colocated region",
          e.getMessage().matches("Region specified in 'colocated-with' .* does not exist.*"));
      caughtIllegalStateException = true;
    }
    assertTrue(caughtIllegalStateException);
  }

  /**
   * Test method for {@link com.gemstone.gemfire.internal.cache.ColocationHelper#getColocatedRegion(com.gemstone.gemfire.internal.cache.PartitionedRegion)}.
   */
  @Test
  public void testGetColocatedRegionLogsWarningForMissingRegionWhenPRConfigHasRegion() {
    when(pr.getCache()).thenReturn(cache);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true)).thenReturn(prRoot);
    when(pr.getPartitionAttributes()).thenReturn(pa);
    when(pr.getFullPath()).thenReturn("/region1");
    when(pa.getColocatedWith()).thenReturn("region2");
    when(((Region)prRoot).get("#region2")).thenReturn(prc);

    PartitionedRegion colocatedPR = null;
    boolean caughtIllegalStateException = false;
    try {
      colocatedPR = ColocationHelper.getColocatedRegion(pr);
    } catch (Exception e) {
      assertEquals("Expected IllegalStateException for missing colocated parent region", IllegalStateException.class, e.getClass());
      assertTrue("Expected IllegalStateException to be thrown for missing colocated region",
          e.getMessage().matches("Region specified in 'colocated-with' .* does not exist.*"));
      caughtIllegalStateException = true;
    }
    assertTrue(caughtIllegalStateException);
  }
}