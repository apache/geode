/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import java.util.Set;

import org.apache.geode.CancelException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.directory.DumpDirectoryFiles;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingFixedResolver;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingResolver;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;

public class LuceneIndexForPartitionedRegion extends LuceneIndexImpl {
  protected Region fileAndChunkRegion;
  protected final FileSystemStats fileSystemStats;

  public static final String FILES_REGION_SUFFIX = ".files";

  public LuceneIndexForPartitionedRegion(String indexName, String regionPath, InternalCache cache) {
    super(indexName, regionPath, cache);

    final String statsName = indexName + "-" + regionPath;
    this.fileSystemStats = new FileSystemStats(cache.getDistributedSystem(), statsName);
  }

  protected RepositoryManager createRepositoryManager(LuceneSerializer luceneSerializer) {
    LuceneSerializer mapper = luceneSerializer;
    if (mapper == null) {
      mapper = new HeterogeneousLuceneSerializer();
    }
    PartitionedRepositoryManager partitionedRepositoryManager =
        new PartitionedRepositoryManager(this, mapper);
    return partitionedRepositoryManager;
  }

  protected void createLuceneListenersAndFileChunkRegions(
      AbstractPartitionedRepositoryManager partitionedRepositoryManager) {
    partitionedRepositoryManager.setUserRegionForRepositoryManager();
    RegionShortcut regionShortCut;
    final boolean withPersistence = withPersistence();
    RegionAttributes regionAttributes = dataRegion.getAttributes();
    final boolean withStorage = regionAttributes.getPartitionAttributes().getLocalMaxMemory() > 0;

    // TODO: 1) dataRegion should be withStorage
    // 2) Persistence to Persistence
    // 3) Replicate to Replicate, Partition To Partition
    // 4) Offheap to Offheap
    if (!withStorage) {
      regionShortCut = RegionShortcut.PARTITION_PROXY;
    } else if (withPersistence) {
      // TODO: add PartitionedRegionAttributes instead
      regionShortCut = RegionShortcut.PARTITION_PERSISTENT;
    } else {
      regionShortCut = RegionShortcut.PARTITION;
    }

    // create PR fileAndChunkRegion, but not to create its buckets for now
    final String fileRegionName = createFileRegionName();
    PartitionAttributes partitionAttributes = dataRegion.getPartitionAttributes();
    DistributionManager dm = this.cache.getInternalDistributedSystem().getDistributionManager();
    LuceneBucketListener lucenePrimaryBucketListener =
        new LuceneBucketListener(partitionedRepositoryManager, dm);

    if (!fileRegionExists(fileRegionName)) {
      fileAndChunkRegion = createFileRegion(regionShortCut, fileRegionName, partitionAttributes,
          regionAttributes, lucenePrimaryBucketListener);
    }

    fileSystemStats
        .setBytesSupplier(() -> getFileAndChunkRegion().getPrStats().getDataStoreBytesInUse());

  }

  public PartitionedRegion getFileAndChunkRegion() {
    return (PartitionedRegion) fileAndChunkRegion;
  }

  public FileSystemStats getFileSystemStats() {
    return fileSystemStats;
  }

  boolean fileRegionExists(String fileRegionName) {
    return cache.getRegion(fileRegionName) != null;
  }

  Region createFileRegion(final RegionShortcut regionShortCut, final String fileRegionName,
      final PartitionAttributes partitionAttributes, final RegionAttributes regionAttributes,
      PartitionListener listener) {
    return createRegion(fileRegionName, regionShortCut, this.regionPath, partitionAttributes,
        regionAttributes, listener);
  }

  public String createFileRegionName() {
    return LuceneServiceImpl.getUniqueIndexRegionName(indexName, regionPath, FILES_REGION_SUFFIX);
  }

  private PartitionAttributesFactory configureLuceneRegionAttributesFactory(
      PartitionAttributesFactory attributesFactory,
      PartitionAttributes<?, ?> dataRegionAttributes) {
    attributesFactory.setTotalNumBuckets(dataRegionAttributes.getTotalNumBuckets());
    attributesFactory.setRedundantCopies(dataRegionAttributes.getRedundantCopies());
    attributesFactory.setPartitionResolver(getPartitionResolver(dataRegionAttributes));
    attributesFactory.setRecoveryDelay(dataRegionAttributes.getRecoveryDelay());
    attributesFactory.setStartupRecoveryDelay(dataRegionAttributes.getStartupRecoveryDelay());
    return attributesFactory;
  }

  private PartitionResolver getPartitionResolver(PartitionAttributes dataRegionAttributes) {
    if (dataRegionAttributes.getPartitionResolver() instanceof FixedPartitionResolver) {
      return new BucketTargetingFixedResolver();
    } else {
      return new BucketTargetingResolver();
    }
  }

  protected <K, V> Region<K, V> createRegion(final String regionName,
      final RegionShortcut regionShortCut, final String colocatedWithRegionName,
      final PartitionAttributes partitionAttributes, final RegionAttributes regionAttributes,
      PartitionListener lucenePrimaryBucketListener) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    if (lucenePrimaryBucketListener != null) {
      partitionAttributesFactory.addPartitionListener(lucenePrimaryBucketListener);
    }
    partitionAttributesFactory.setColocatedWith(colocatedWithRegionName);
    configureLuceneRegionAttributesFactory(partitionAttributesFactory, partitionAttributes);

    // Create AttributesFactory based on input RegionShortcut
    RegionAttributes baseAttributes = this.cache.getRegionAttributes(regionShortCut.toString());
    AttributesFactory factory = new AttributesFactory(baseAttributes);
    factory.setPartitionAttributes(partitionAttributesFactory.create());
    if (regionAttributes.getDataPolicy().withPersistence()) {
      factory.setDiskStoreName(regionAttributes.getDiskStoreName());
    }
    RegionAttributes<K, V> attributes = factory.create();

    return createRegion(regionName, attributes);
  }

  public void close() {}

  @Override
  public void dumpFiles(final String directory) {
    ResultCollector results = FunctionService.onRegion(getDataRegion())
        .setArguments(new String[] {directory, indexName}).execute(DumpDirectoryFiles.ID);
    results.getResult();
  }

  @Override
  public void destroy(boolean initiator) {
    if (logger.isDebugEnabled()) {
      logger.debug("Destroying index regionPath=" + regionPath + "; indexName=" + indexName
          + "; initiator=" + initiator);
    }

    // Invoke super destroy to remove the extension and async event queue
    super.destroy(initiator);

    // Destroy index on remote members if necessary
    if (initiator) {
      destroyOnRemoteMembers();
    }

    // Destroy the file region (colocated with the application region) if necessary
    // localDestroyRegion can't be used because locally destroying regions is not supported on
    // colocated regions
    if (initiator) {
      fileAndChunkRegion.destroyRegion();
      if (logger.isDebugEnabled()) {
        logger.debug("Destroyed fileAndChunkRegion=" + fileAndChunkRegion.getName());
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Destroyed index regionPath=" + regionPath + "; indexName=" + indexName
          + "; initiator=" + initiator);
    }
  }

  private void destroyOnRemoteMembers() {
    PartitionedRegion pr = (PartitionedRegion) getDataRegion();
    DistributionManager dm = pr.getDistributionManager();
    Set<InternalDistributedMember> recipients = pr.getRegionAdvisor().adviseAllPRNodes();
    if (!recipients.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("LuceneIndexForPartitionedRegion: About to send destroy message recipients="
            + recipients);
      }
      ReplyProcessor21 processor = new ReplyProcessor21(dm, recipients);
      DestroyLuceneIndexMessage message = new DestroyLuceneIndexMessage(recipients,
          processor.getProcessorId(), regionPath, indexName);
      dm.putOutgoing(message);
      if (logger.isDebugEnabled()) {
        logger.debug("LuceneIndexForPartitionedRegion: Sent message recipients=" + recipients);
      }
      try {
        processor.waitForReplies();
      } catch (ReplyException e) {
        if (!(e.getCause() instanceof CancelException)) {
          throw e;
        }
      } catch (InterruptedException e) {
        dm.getCancelCriterion().checkCancelInProgress(e);
        Thread.currentThread().interrupt();
      }
    }
  }
}
