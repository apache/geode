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

package org.apache.geode.management.internal.configuration.realizers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.runtime.PdxInfo;

public class PdxRealizerTest {
  private PdxRealizer pdxRealizer;
  private InternalCache cache;

  @Before
  public void before() throws Exception {
    pdxRealizer = new PdxRealizer();
    cache = mock(InternalCache.class);
  }

  @Test
  public void getPdxInformation() {
    when(cache.getPdxReadSerialized()).thenReturn(true);
    PdxInfo pdxInfo = pdxRealizer.get(null, cache);
    assertThat(pdxInfo.isReadSerialized()).isTrue();
    assertThat(pdxInfo.isIgnoreUnreadFields()).isFalse();
  }

  @Test
  public void readOnly() {
    assertThat(pdxRealizer.isReadyOnly()).isTrue();
    assertThatThrownBy(() -> pdxRealizer.create(null, cache))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> pdxRealizer.delete(null, cache))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> pdxRealizer.update(null, cache))
        .isInstanceOf(IllegalStateException.class);
  }
}
