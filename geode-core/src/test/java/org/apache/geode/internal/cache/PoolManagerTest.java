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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.client.internal.PoolImpl;

public class PoolManagerTest {
  private PoolImpl pool;
  private PoolManagerImpl poolManager;

  @Before
  public void setUp() {
    pool = mock(PoolImpl.class);
    poolManager = spy(new PoolManagerImpl(true));

    assertThat(poolManager.getMap()).isEmpty();
  }

  @Test
  public void unregisterShouldThrowExceptionWhenPoolHasRegionsStillAssociated() {
    when(pool.getAttachCount()).thenReturn(2);

    assertThatThrownBy(() -> poolManager.unregister(pool)).isInstanceOf(IllegalStateException.class)
        .hasMessage("Pool could not be destroyed because it is still in use by 2 regions");
  }

  @Test
  public void unregisterShouldReturnFalseWhenThePoolIsNotPartOfTheManagedPools() {
    when(pool.getAttachCount()).thenReturn(0);

    assertThat(poolManager.unregister(pool)).isFalse();
  }

  @Test
  public void unregisterShouldReturnTrueWhenThePoolIsSuccessfullyRemovedFromTheManagedPools() {
    when(pool.getAttachCount()).thenReturn(0);
    poolManager.register(pool);
    assertThat(poolManager.getMap()).isNotEmpty();

    assertThat(poolManager.unregister(pool)).isTrue();
    assertThat(poolManager.getMap()).isEmpty();
  }
}
