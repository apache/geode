package com.gemstone.gemfire.cache.lucene.internal;

import java.io.IOException;

import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystemStats;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepositoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

public class IndexRepositoryFactory {

  public IndexRepositoryFactory() {
  }

  public IndexRepository createIndexRepository(final Integer bucketId,
                                        PartitionedRegion userRegion,
                                        PartitionedRegion fileRegion,
                                        PartitionedRegion chunkRegion,
                                        LuceneSerializer serializer,
                                        Analyzer analyzer,
                                        LuceneIndexStats indexStats,
                                        FileSystemStats fileSystemStats)
    throws BucketNotFoundException, IOException
  {
    final IndexRepository repo;
    BucketRegion fileBucket = getMatchingBucket(fileRegion, bucketId);
    BucketRegion chunkBucket = getMatchingBucket(chunkRegion, bucketId);
    RegionDirectory dir = new RegionDirectory(fileBucket, chunkBucket, fileSystemStats);
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    IndexWriter writer = new IndexWriter(dir, config);
    repo = new IndexRepositoryImpl(fileBucket, writer, serializer, indexStats);
    return repo;
  }

  /**
   * Find the bucket in region2 that matches the bucket id from region1.
   */
  private BucketRegion getMatchingBucket(PartitionedRegion region, Integer bucketId) throws BucketNotFoundException {
    //Force the bucket to be created if it is not already
    region.getOrCreateNodeForBucketWrite(bucketId, null);

    BucketRegion result = region.getDataStore().getLocalBucketById(bucketId);
    if (result == null) {
      throw new BucketNotFoundException("Bucket not found for region " + region + " bucekt id " + bucketId);
    }

    return result;
  }
}