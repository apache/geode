package com.gemstone.gemfire.cache.lucene.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepositoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteWeakHashMap;

/**
 * Manages index repositories for partitioned regions.
 * 
 * This class lazily creates the IndexRepository for each individual
 * bucket. If a Bucket is rebalanced, this class will create a new
 * index repository when the bucket returns to this node.
 */
public class PartitionedRepositoryManager implements RepositoryManager {
  /** map of the parent bucket region to the index repository 
   * 
   * This is based on the BucketRegion in case a bucket is rebalanced, we don't want to 
   * return a stale index repository. If a bucket moves off of this node and
   * comes back, it will have a new BucketRegion object.
   * 
   * It is weak so that the old BucketRegion will be garbage collected. 
   */
  CopyOnWriteWeakHashMap<BucketRegion, IndexRepository> indexRepositories = new CopyOnWriteWeakHashMap<BucketRegion, IndexRepository>();
  
  /** The user region for this index */
  private final PartitionedRegion userRegion;
  
  private final PartitionedRegion fileRegion;
  private final PartitionedRegion chunkRegion;
  private final LuceneSerializer serializer;
  private final Analyzer analyzer;
  
  /**
   * 
   * @param userRegion The user partition region
   * @param fileRegion The partition region used for file metadata. Should be colocated with the user pr
   * @param chunkRegion The partition region users for chunk metadata.
   * @param serializer The serializer that should be used for converting objects to lucene docs.
   */
  public PartitionedRepositoryManager(PartitionedRegion userRegion, PartitionedRegion fileRegion,
      PartitionedRegion chunkRegion,
      LuceneSerializer serializer,
      Analyzer analyzer) {
    this.userRegion = userRegion;
    this.fileRegion = fileRegion;
    this.chunkRegion = chunkRegion;
    this.serializer = serializer;
    this.analyzer = analyzer;
  }

  @Override
  public IndexRepository getRepository(Region region, Object key, Object callbackArg) throws BucketNotFoundException {
    BucketRegion userBucket = userRegion.getBucketRegion(key, callbackArg);
    if(userBucket == null) {
      throw new BucketNotFoundException("User bucket was not found for region " + region + "key " +  key + " callbackarg " + callbackArg);
    }
    
    return getRepository(userBucket);
  }
  
  @Override
  public Collection<IndexRepository> getRepositories(Region region) throws BucketNotFoundException {
    if(!(region instanceof LocalDataSet)) {
      throw new IllegalStateException("Trying to find the repositories for a region which is not the local data set of a function");
    }
    
    LocalDataSet dataSet = (LocalDataSet) region;
    Set<Integer> buckets = dataSet.getBucketSet();
    ArrayList<IndexRepository> repos = new ArrayList<IndexRepository>(buckets.size());
    for(Integer bucketId : buckets) {
      BucketRegion userBucket = userRegion.getDataStore().getLocalBucketById(bucketId);
      if(userBucket == null) {
        throw new BucketNotFoundException("User bucket was not found for region " + region + "bucket id " + bucketId);
      } else {
        repos.add(getRepository(userBucket));
      }
    }

    return repos;
  }

  /**
   * Return the repository for a given user bucket
   */
  private IndexRepository getRepository(BucketRegion userBucket) throws BucketNotFoundException {
    IndexRepository repo = indexRepositories.get(userBucket);
    if(repo == null) {
      try {
        RegionDirectory dir = new RegionDirectory(getMatchingBucket(userBucket, fileRegion), getMatchingBucket(userBucket, chunkRegion));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, config);
        repo = new IndexRepositoryImpl(writer, serializer);
        IndexRepository oldRepo = indexRepositories.putIfAbsent(userBucket, repo);
        if(oldRepo != null) {
          repo = oldRepo;
        }
      } catch(IOException e) {
        throw new InternalGemFireError("Unable to create index repository", e);
      }
    }
    
    return repo;
  }

  /**
   * Find the bucket in region2 that matches the bucket id from region1.
   */
  private BucketRegion getMatchingBucket(BucketRegion region1, PartitionedRegion region2) throws BucketNotFoundException {
    BucketRegion result = region2.getDataStore().getLocalBucketById(region1.getId());
    if(result == null) {
      throw new BucketNotFoundException("Bucket not found for region " + region2 + " bucekt id " + region1.getId());
    }
    
    return result;
  }
}
