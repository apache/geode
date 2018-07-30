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

import static org.apache.geode.internal.cache.InitialImageOperation.GIIStatus.GOTIMAGE_BY_FULLGII;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.cache.persistence.DiskExceptionHandler;

public class DiskRegionTest {

  private DiskRegion diskRegion;

  @Before
  public void setup() {
    DiskStoreImpl diskStoreImpl = mock(DiskStoreImpl.class);
    when(diskStoreImpl.getDiskInitFile()).thenReturn(mock(DiskInitFile.class));
    when(diskStoreImpl.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    diskRegion = spy(new DiskRegion(diskStoreImpl, "testRegion",
        false, true, false, false,
        mock(DiskRegionStats.class), mock(CancelCriterion.class), mock(DiskExceptionHandler.class),
        null, mock(EnumSet.class), null, 0,
        null, false));
  }

  @Test
  public void finishInitializeOwnerOnAsyncPersistentRegionWithUnTrustedRvvCallsWriteRVV() {
    diskRegion.setRVVTrusted(false);

    diskRegion.finishInitializeOwner(mock(LocalRegion.class), GOTIMAGE_BY_FULLGII);

    verify(diskRegion, times(1)).writeRVV(any(), any());
  }

  @Test
  public void finishInitializeOwnerOnAsyncPersistentRegionWithTrustedRvvNeverCallsWriteRVV() {
    diskRegion.setRVVTrusted(true);

    diskRegion.finishInitializeOwner(mock(LocalRegion.class), GOTIMAGE_BY_FULLGII);

    verify(diskRegion, never()).writeRVV(any(), any());
  }

}
