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
package org.apache.geode.internal.cache.lru;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class LRUAlgorithmTest {

  @Test
  public void shouldBeMockable() throws Exception {
    LRUAlgorithm mockLRUAlgorithm = mock(LRUAlgorithm.class);
    EnableLRU mockEnableLRU = mock(EnableLRU.class);
    when(mockLRUAlgorithm.getLRUHelper()).thenReturn(mockEnableLRU);
    assertThat(mockLRUAlgorithm.getLRUHelper()).isEqualTo(mockEnableLRU);
  }
}
