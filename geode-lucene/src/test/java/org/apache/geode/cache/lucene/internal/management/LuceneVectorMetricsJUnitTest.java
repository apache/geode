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
package org.apache.geode.cache.lucene.internal.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.management.LuceneIndexMetrics;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneVectorMetricsJUnitTest {

  @Test
  public void luceneIndexStatsTracksSegmentMemoryBytesAndVectorFieldCount() {
    StatisticsFactory factory = mock(StatisticsFactory.class);
    Statistics statistics = mock(Statistics.class);
    when(factory.createAtomicStatistics(any(), anyString())).thenReturn(statistics);

    LuceneIndexStats stats = new LuceneIndexStats(factory, "vec-region");
    ArgumentCaptor<StatisticsType> typeCaptor = ArgumentCaptor.forClass(StatisticsType.class);
    verify(factory).createAtomicStatistics(typeCaptor.capture(), eq("vec-region"));
    StatisticsType type = typeCaptor.getValue();

    stats.setSegmentMemoryBytesSupplier(() -> 131072L);
    int segBytesId = type.nameToId(StatsKey.SEGMENT_MEMORY_BYTES);
    ArgumentCaptor<LongSupplier> supplierCaptor = ArgumentCaptor.forClass(LongSupplier.class);
    verify(statistics).setLongSupplier(eq(segBytesId), supplierCaptor.capture());
    assertThat(supplierCaptor.getValue().getAsLong()).isEqualTo(131072L);

    stats.setVectorFieldCount(2);
    int vecCountId = type.nameToId(StatsKey.VECTOR_FIELD_COUNT);
    verify(statistics).setInt(eq(vecCountId), eq(2));
  }

  @Test
  public void offHeapVectorBytesEstimateReflectsSq8CompressionAndGraphLinks() {
    Map<String, VectorIndexMetadata> fp32 = Collections.singletonMap("emb",
        new VectorIndexMetadata("emb", 128, VectorSimilarityFunction.COSINE, 16, 100, false));
    Map<String, VectorIndexMetadata> sq8 = Collections.singletonMap("emb",
        new VectorIndexMetadata("emb", 128, VectorSimilarityFunction.COSINE, 16, 100, true));

    // FP32: 1000 * (128 * 4 + 2 * 16 * 4) = 1000 * (512 + 128) = 640,000
    assertThat(LuceneIndexMetrics.estimateOffHeapVectorBytes(1000, fp32)).isEqualTo(640_000L);
    // SQ8: 1000 * (128 * 1 + 2 * 16 * 4) = 1000 * (128 + 128) = 256,000
    assertThat(LuceneIndexMetrics.estimateOffHeapVectorBytes(1000, sq8)).isEqualTo(256_000L);
  }

  @Test
  public void statsMonitorPopulatesSegmentMemoryAndVectorDescriptorsOnMetrics() {
    StatisticsFactory factory = mock(StatisticsFactory.class);
    Statistics statistics = mock(Statistics.class);
    when(factory.createAtomicStatistics(any(), anyString())).thenReturn(statistics);

    LuceneIndexStats indexStats = new LuceneIndexStats(factory, "vec-idx");
    LuceneIndexImpl index = mock(LuceneIndexImpl.class);
    when(index.getRegionPath()).thenReturn("/docs");
    when(index.getName()).thenReturn("vecIdx");
    when(index.getIndexStats()).thenReturn(indexStats);

    Map<String, VectorIndexMetadata> vectorFields = new LinkedHashMap<>();
    vectorFields.put("embedding",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, 16, 100, true));
    when(index.getVectorFields()).thenReturn(vectorFields);

    LuceneIndexStatsMonitor monitor = new LuceneIndexStatsMonitor(index) {
      @Override
      public void addStatisticsToMonitor(Statistics stats) {
        // No live StatisticsSampler in unit tests
      }

      @Override
      public Number getStatistic(String statName) {
        if (StatsKey.DOCUMENTS.equals(statName)) {
          return 500;
        }
        if (StatsKey.SEGMENT_MEMORY_BYTES.equals(statName)) {
          return 262144L;
        }
        return 0;
      }
    };

    LuceneIndexMetrics metrics = monitor.getIndexMetrics(index);
    assertThat(metrics.getSegmentMemoryBytes()).isEqualTo(262144L);
    assertThat(metrics.getEstimatedOffHeapVectorBytes()).isEqualTo(500L * (128L + 128L));
    assertThat(metrics.getVectorFields())
        .containsExactly("embedding(dim=128,similarity=COSINE,M=16,efC=100,sq8=true)");
  }

  @Test
  public void legacySixteenArgMetricsConstructorRemainsBackwardCompatible() {
    LuceneIndexMetrics metrics = new LuceneIndexMetrics("/r", "idx", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14);
    assertThat(metrics.getRegionPath()).isEqualTo("/r");
    assertThat(metrics.getIndexName()).isEqualTo("idx");
    assertThat(metrics.getDocuments()).isEqualTo(14);
    assertThat(metrics.getSegmentMemoryBytes()).isEqualTo(0L);
    assertThat(metrics.getEstimatedOffHeapVectorBytes()).isEqualTo(0L);
    assertThat(metrics.getVectorFields()).isEmpty();
  }
}
