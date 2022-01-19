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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheClosedException;

public class TXRegionLockRequestImplTest {

  private InternalCache cache;
  private LocalRegion region;
  private CancelCriterion cancelCriterion;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    region = mock(LocalRegion.class);
    cancelCriterion = mock(CancelCriterion.class);

    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    doThrow(new CacheClosedException()).when(cancelCriterion).checkCancelInProgress(any());
  }

  @Test
  public void getKeysThrowsCancelExceptionIfCacheIsClosed() {
    TXRegionLockRequestImpl txRegionLockRequest = new TXRegionLockRequestImpl(cache, region);
    assertThatThrownBy(txRegionLockRequest::getKeys).isInstanceOf(CancelException.class);
  }
}
