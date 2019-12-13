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

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;

public class IndexRealizerTest {
  InternalCache cache;
  InternalQueryService queryService;
  IndexRealizer indexRealizer;
  Index index;

  @Before
  public void init() {
    cache = mock(InternalCache.class);
    queryService = mock(InternalQueryService.class);
    when(cache.getQueryService()).thenReturn(queryService);
    indexRealizer = new IndexRealizer();
    index = new Index();
    index.setName("testIndex");
    index.setRegionPath("testRegion");
    index.setExpression("test Expression");
  }

  @Test
  public void create_succeeds_with_key() throws Exception {
    index.setIndexType(IndexType.KEY);
    RealizationResult realizationResult = indexRealizer.create(index, cache);
    assertSoftly(softly -> {
      softly.assertThat(realizationResult.isSuccess()).isTrue();
      softly.assertThat(realizationResult.getMessage()).contains("testIndex successfully created");
    });
    verify(queryService).createKeyIndex("testIndex", "test Expression", "testRegion");
  }

  @Test
  public void create_succeeds_with_functional() throws Exception {
    index.setIndexType(IndexType.RANGE);
    RealizationResult realizationResult = indexRealizer.create(index, cache);
    assertSoftly(softly -> {
      softly.assertThat(realizationResult.isSuccess()).isTrue();
      softly.assertThat(realizationResult.getMessage()).contains("testIndex successfully created");
    });
    verify(queryService).createIndex("testIndex", "test Expression", "testRegion");
  }

  @Test
  public void create_fails_with_queryServerice_error() throws Exception {
    index.setIndexType(IndexType.RANGE);
    doThrow(new UnsupportedOperationException("I do not support this operation"))
        .when(queryService).createIndex("testIndex", "test Expression", "testRegion");
    RealizationResult realizationResult = indexRealizer.create(index, cache);
    assertSoftly(softly -> {
      softly.assertThat(realizationResult.isSuccess()).isFalse();
      softly.assertThat(realizationResult.getMessage()).contains("I do not support this operation");
    });
  }

}
