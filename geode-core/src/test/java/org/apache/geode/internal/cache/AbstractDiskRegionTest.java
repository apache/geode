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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;


public class AbstractDiskRegionTest {

  @Test
  public void shouldBeMockable() throws Exception {
    AbstractDiskRegion mockAbstractDiskRegion = mock(AbstractDiskRegion.class);
    DiskStoreImpl mockDiskStoreImpl = mock(DiskStoreImpl.class);
    DiskId mockDiskId = mock(DiskId.class);
    Object object = new Object();

    when(mockAbstractDiskRegion.getDiskStore()).thenReturn(mockDiskStoreImpl);
    when(mockAbstractDiskRegion.getRaw(eq(mockDiskId))).thenReturn(object);

    assertThat(mockAbstractDiskRegion.getDiskStore()).isEqualTo(mockDiskStoreImpl);
    assertThat(mockAbstractDiskRegion.getRaw(mockDiskId)).isEqualTo(object);
  }
}
