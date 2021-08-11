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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DiskStoreFactory;

public class OverflowOplogTest {

  private OverflowOplog oplogMock;

  @Before
  public void setup() {
    DiskStoreImpl parentDiskStoreMock = mock(DiskStoreImpl.class);
    when(parentDiskStoreMock.getWriteBufferSize())
        .thenReturn(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE);

    oplogMock = mock(OverflowOplog.class);
    when(oplogMock.getParent()).thenReturn(parentDiskStoreMock);
    when(oplogMock.getWriteBufferCapacity()).thenCallRealMethod();
  }

  @Test
  public void writeBufferSizeValueIsObtainedFromParentIfSystemPropertyNotDefined() {
    when(oplogMock.getWriteBufferSizeProperty()).thenReturn(null);
    assertThat(oplogMock.getWriteBufferCapacity())
        .isEqualTo(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE);
  }

  @Test
  public void writeBufferSizeValueIsObtainedFromSystemPropertyWhenDefined() {
    when(oplogMock.getWriteBufferSizeProperty()).thenReturn(12345);
    assertThat(oplogMock.getWriteBufferCapacity()).isEqualTo(12345);
  }
}
