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
package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.cache.lucene.internal.IndexRepositoryFactory.GET_INDEX_WRITER_MAX_ATTEMPTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;

public class IndexRepositoryFactoryTest {
  private Integer bucketId;
  private LuceneSerializer<?> serializer;
  private PartitionedRegion userRegion;
  private PartitionedRegion fileRegion;
  private IndexRepository oldRepository;
  private BucketRegion fileAndChunkBucket;
  private BucketAdvisor fileAndChunkBucketAdvisor;
  private LuceneIndexForPartitionedRegion luceneIndex;
  private IndexRepositoryFactory indexRepositoryFactory;
  private DistributedLockService distributedLockService;

  @Before
  public void setUp() {
    bucketId = 0;
    serializer = mock(LuceneSerializer.class);
    userRegion = mock(PartitionedRegion.class);
    fileRegion = mock(PartitionedRegion.class);
    oldRepository = mock(IndexRepository.class);
    fileAndChunkBucket = mock(BucketRegion.class);
    fileAndChunkBucketAdvisor = mock(BucketAdvisor.class);
    luceneIndex = mock(LuceneIndexForPartitionedRegion.class);
    indexRepositoryFactory = spy(IndexRepositoryFactory.class);
    distributedLockService = mock(DistributedLockService.class);

    when(luceneIndex.getFileAndChunkRegion()).thenReturn(fileRegion);
    when(userRegion.getCache()).thenReturn(mock(InternalCache.class));
    when(userRegion.getRegionService()).thenReturn(mock(InternalCache.class));
    when(indexRepositoryFactory.getLockService()).thenReturn(distributedLockService);
    when(fileAndChunkBucket.getBucketAdvisor()).thenReturn(fileAndChunkBucketAdvisor);

    doReturn(fileAndChunkBucket).when(indexRepositoryFactory).getMatchingBucket(fileRegion,
        bucketId);
    doReturn(mock(BucketRegion.class)).when(indexRepositoryFactory).getMatchingBucket(userRegion,
        bucketId);
  }

  @Test
  public void finishComputingRepositoryShouldReturnNullAndCleanOldRepositoryWhenFileAndChunkBucketIsNull()
      throws IOException {
    doReturn(null).when(indexRepositoryFactory).getMatchingBucket(fileRegion, bucketId);

    IndexRepository indexRepository = indexRepositoryFactory.finishComputingRepository(0,
        serializer, userRegion, oldRepository, luceneIndex);
    assertThat(indexRepository).isNull();
    verify(oldRepository).cleanup();
  }

  @Test
  public void finishComputingRepositoryShouldReturnNullAndCleanOldRepositoryWhenFileAndChunkBucketIsNotPrimary()
      throws IOException {
    when(fileAndChunkBucketAdvisor.isPrimary()).thenReturn(false);

    IndexRepository indexRepository = indexRepositoryFactory.finishComputingRepository(0,
        serializer, userRegion, oldRepository, luceneIndex);
    assertThat(indexRepository).isNull();
    verify(oldRepository).cleanup();
  }

  @Test
  public void finishComputingRepositoryShouldReturnOldRepositoryWhenNotNullAndNotClosed()
      throws IOException {
    when(oldRepository.isClosed()).thenReturn(false);
    when(fileAndChunkBucketAdvisor.isPrimary()).thenReturn(true);

    IndexRepository indexRepository = indexRepositoryFactory.finishComputingRepository(0,
        serializer, userRegion, oldRepository, luceneIndex);
    assertThat(indexRepository).isNotNull();
    assertThat(indexRepository).isSameAs(oldRepository);
  }

  @Test
  public void finishComputingRepositoryShouldReturnNullWhenLockCanNotBeAcquiredAndFileAndChunkBucketIsNotPrimary()
      throws IOException {
    when(oldRepository.isClosed()).thenReturn(true);
    when(fileAndChunkBucketAdvisor.isPrimary()).thenReturn(true).thenReturn(false);
    when(distributedLockService.lock(any(), anyLong(), anyLong())).thenReturn(false);

    IndexRepository indexRepository = indexRepositoryFactory.finishComputingRepository(0,
        serializer, userRegion, oldRepository, luceneIndex);
    assertThat(indexRepository).isNull();
  }

  @Test
  public void finishComputingRepositoryShouldThrowExceptionAndReleaseLockWhenIOExceptionIsThrownWhileBuildingTheIndex()
      throws IOException {
    when(oldRepository.isClosed()).thenReturn(true);
    when(fileAndChunkBucketAdvisor.isPrimary()).thenReturn(true);
    when(distributedLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
    doThrow(new IOException("Test Exception")).when(indexRepositoryFactory)
        .buildIndexWriter(bucketId, fileAndChunkBucket, luceneIndex);

    assertThatThrownBy(() -> indexRepositoryFactory.finishComputingRepository(0,
        serializer, userRegion, oldRepository, luceneIndex)).isInstanceOf(IOException.class);
    verify(distributedLockService).unlock(any());
  }

  @Test
  public void finishComputingRepositoryShouldThrowExceptionAndReleaseLockWhenCacheClosedExceptionIsThrownWhileBuildingTheIndex()
      throws IOException {
    when(oldRepository.isClosed()).thenReturn(true);
    when(fileAndChunkBucketAdvisor.isPrimary()).thenReturn(true);
    when(distributedLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
    doThrow(new CacheClosedException("Test Exception")).when(indexRepositoryFactory)
        .buildIndexWriter(bucketId, fileAndChunkBucket, luceneIndex);

    assertThatThrownBy(() -> indexRepositoryFactory.finishComputingRepository(0, serializer,
        userRegion, oldRepository, luceneIndex)).isInstanceOf(CacheClosedException.class);
    verify(distributedLockService).unlock(any());
  }

  @Test
  public void buildIndexWriterRetriesCreatingIndexWriterWhenIOExceptionEncountered()
      throws IOException {
    IndexWriter writer = mock(IndexWriter.class);
    doThrow(new IOException()).doReturn(writer).when(indexRepositoryFactory).getIndexWriter(any(),
        any());
    assertThat(indexRepositoryFactory.buildIndexWriter(bucketId, fileAndChunkBucket, luceneIndex))
        .isEqualTo(writer);
    verify(indexRepositoryFactory, times(2)).getIndexWriter(any(), any());
  }

  @Test
  public void buildIndexWriterThrowsExceptionWhenIOExceptionConsistentlyEncountered()
      throws IOException {
    IOException testException = new IOException("Test exception");
    doThrow(testException).when(indexRepositoryFactory).getIndexWriter(any(), any());
    assertThatThrownBy(
        () -> indexRepositoryFactory.buildIndexWriter(bucketId, fileAndChunkBucket, luceneIndex))
            .isEqualTo(testException);
    verify(indexRepositoryFactory, times(GET_INDEX_WRITER_MAX_ATTEMPTS)).getIndexWriter(any(),
        any());
  }
}
