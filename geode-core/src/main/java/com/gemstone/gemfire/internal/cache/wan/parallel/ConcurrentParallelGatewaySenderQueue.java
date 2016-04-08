/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.wan.parallel;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSGatewayEventImpl;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.gemstone.gemfire.internal.size.SingleObjectSizer;

/**
 * Queue built on top of {@link ParallelGatewaySenderQueue} which allows
 * multiple dispatcher to register and do peek/remove from the 
 * underlying {@link ParallelGatewaySenderQueue} 
 * 
 * There is only one queue, but this class co-ordinates access
 * by multiple threads such that we get zero contention while peeking
 * or removing.
 * 
 * It implements RegionQueue so that AbstractGatewaySenderEventProcessor
 * can work on it.
 *   
 *
 */
public class ConcurrentParallelGatewaySenderQueue implements RegionQueue {

  private final ParallelGatewaySenderEventProcessor processors[];
  
  public ConcurrentParallelGatewaySenderQueue(
		  ParallelGatewaySenderEventProcessor pro[]) {
    this.processors = pro;
  }
  
  @Override
  public void put(Object object) throws InterruptedException, CacheException {
    throw new UnsupportedOperationException("CPGAQ method(put) is not supported");
  }
  
  @Override
  public void close() {
    /*
    this.commonQueue.close();
    // no need to free peekedEvents since they all had makeOffHeap called on them.
	  throw new UnsupportedOperationException("CPGAQ method(close) is not supported");
    */
  }

  @Override
  public Region getRegion() {
	  return this.processors[0].getQueue().getRegion();
  }
  
  public PartitionedRegion getRegion(String fullpath) {
    return processors[0].getRegion(fullpath);
  }
  
  public Set<PartitionedRegion> getRegions() {
	return ((ParallelGatewaySenderQueue)(processors[0].getQueue())).getRegions();
  }

  @Override
  public Object take() throws CacheException, InterruptedException {
	throw new UnsupportedOperationException("This method(take) is not suported");
  }

  @Override
  public List take(int batchSize) throws CacheException, InterruptedException {
	throw new UnsupportedOperationException("This method(take) is not suported");
  }

  @Override
  public void remove() throws CacheException {
	  throw new UnsupportedOperationException("This method(remove) is not suported");      
  }

  @Override
  public Object peek() throws InterruptedException, CacheException {
	  throw new UnsupportedOperationException("This method(peek) is not suported");
  }

  @Override
  public List peek(int batchSize) throws InterruptedException, CacheException {
	  throw new UnsupportedOperationException("This method(peek) is not suported");
  }

  @Override
  public List peek(int batchSize, int timeToWait) throws InterruptedException,
      CacheException {
    throw new UnsupportedOperationException("This method(peek) is not suported");  
  }

  @Override
  public int size() {
	//is that fine??
	return this.processors[0].getQueue().size();
  }
  
  public int localSize() {
	return ((ParallelGatewaySenderQueue)(processors[0].getQueue())).localSize();
  }

  @Override
  public void addCacheListener(CacheListener listener) {
	  this.processors[0].getQueue().addCacheListener(listener);    
  }

  @Override
  public void removeCacheListener() {
    this.processors[0].removeCacheListener();    
  }

  @Override
  public void remove(int top) throws CacheException {
	throw new UnsupportedOperationException("This method(remove) is not suported");
  }

/*  public void resetLastPeeked(){
    this.resetLastPeeked = true;
  }*/  
  
  public long estimateMemoryFootprint(SingleObjectSizer sizer) {
	long size = 0;
	for(int i=0; i< processors.length; i++)
	  size += ((ParallelGatewaySenderQueue)this.processors[i].getQueue()).estimateMemoryFootprint(sizer);
	return size;
  }

  /*@Override
  public void release() {
	for(int i =0; i< processors.length; i++){
	  processors[i].getQueue().release();
	}
  }*/
  
  public void removeShadowPR(String prRegionName) {
  	for(int i =0; i< processors.length; i++){
   	 processors[i].removeShadowPR(prRegionName);
    }
  }
  
  public void addShadowPartitionedRegionForUserPR(PartitionedRegion pr) {
	for(int i =0; i< processors.length; i++){
	  processors[i].addShadowPartitionedRegionForUserPR(pr);
	 }
  }
  
  private ParallelGatewaySenderEventProcessor getPGSProcessor(int bucketId) {
  	int index = bucketId % this.processors.length;
  	return processors[index];
  }

  public BlockingQueue<GatewaySenderEventImpl> getBucketTmpQueue(int bucketId) {
   return getPGSProcessor(bucketId).getBucketTmpQueue(bucketId);
  }
  
  public void notifyEventProcessorIfRequired(int bucketId) {
   getPGSProcessor( bucketId).notifyEventProcessorIfRequired(bucketId);
  }
  
  public HDFSBucketRegionQueue getBucketRegionQueue(PartitionedRegion region,
    int bucketId) throws ForceReattemptException {
	return getPGSProcessor(bucketId).getBucketRegionQueue(region, bucketId);
  }
  
  public void clear(PartitionedRegion pr, int bucketId) {
  	getPGSProcessor(bucketId).clear(pr, bucketId);
  }
  
  public void cleanUp() {
	for(int i=0; i< processors.length; i++)
	  ((ParallelGatewaySenderQueue)this.processors[i].getQueue()).cleanUp();
  }
  
  public void conflateEvent(Conflatable conflatableObject, int bucketId,
      Long tailKey) {
  	getPGSProcessor(bucketId).conflateEvent(conflatableObject, bucketId, tailKey);
  }
  
  public HDFSGatewayEventImpl get(PartitionedRegion region, byte[] regionKey,
      int bucketId) throws ForceReattemptException {
    return getPGSProcessor(bucketId).get(region, regionKey, bucketId);
  }
  
  public void addShadowPartitionedRegionForUserRR(DistributedRegion userRegion) {
	for(int i =0; i< processors.length; i++){
  	 processors[i].addShadowPartitionedRegionForUserRR(userRegion);;
   }
  }
  
  public long getNumEntriesInVMTestOnly() {
	return ((ParallelGatewaySenderQueue)(processors[0].getQueue())).getNumEntriesInVMTestOnly();
  }
	 
  public long getNumEntriesOverflowOnDiskTestOnly() {
	return ((ParallelGatewaySenderQueue)(processors[0].getQueue())).getNumEntriesOverflowOnDiskTestOnly();
  }
}
