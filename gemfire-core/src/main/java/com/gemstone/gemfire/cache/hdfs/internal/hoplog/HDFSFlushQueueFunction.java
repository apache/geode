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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.hdfs.internal.FlushObserver.AsyncFlushResult;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.LocalResultCollector;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

public class HDFSFlushQueueFunction implements Function, InternalEntity{
  private static final int MAX_RETRIES = Integer.getInteger("gemfireXD.maxFlushQueueRetries", 3);
  private static final boolean VERBOSE = Boolean.getBoolean("hdfsFlushQueueFunction.VERBOSE");
  private static final Logger logger = LogService.getLogger();
  private static final String ID = HDFSFlushQueueFunction.class.getName();
  
  public static void flushQueue(PartitionedRegion pr, int maxWaitTime) {
    
    Set<Integer> buckets = new HashSet<Integer>(pr.getRegionAdvisor().getBucketSet());

    maxWaitTime *= 1000;
    long start = System.currentTimeMillis();
    
    int retries = 0;
    long remaining = 0;
    while (retries++ < MAX_RETRIES && (remaining = waitTime(start, maxWaitTime)) > 0) {
      if (logger.isDebugEnabled() || VERBOSE) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Flushing buckets " + buckets 
            + ", attempt = " + retries 
            + ", remaining = " + remaining));
      }
      
      HDFSFlushQueueArgs args = new HDFSFlushQueueArgs(buckets, remaining);
      
      HDFSFlushQueueResultCollector rc = new HDFSFlushQueueResultCollector(buckets);
      AbstractExecution exec = (AbstractExecution) FunctionService
          .onRegion(pr)
          .withArgs(args)
          .withCollector(rc);
      exec.setWaitOnExceptionFlag(true);
      
      try {
        exec.execute(ID);
        if (rc.getResult()) {
          if (logger.isDebugEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Flushed all buckets successfully")); 
          }
          return;
        }
      } catch (FunctionException e) {
        if (logger.isDebugEnabled() || VERBOSE) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Encountered error flushing queue"), e); 
        }
      }
      
      buckets.removeAll(rc.getSuccessfulBuckets());
      for (int bucketId : buckets) {
        remaining = waitTime(start, maxWaitTime);
        if (logger.isDebugEnabled() || VERBOSE) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Waiting for bucket " + bucketId)); 
        }
        pr.getNodeForBucketWrite(bucketId, new PartitionedRegion.RetryTimeKeeper((int) remaining));
      }
    }
    
    pr.checkReadiness();
    throw new FunctionException("Unable to flush the following buckets: " + buckets);
  }
  
  private static long waitTime(long start, long max) {
    if (max == 0) {
      return Integer.MAX_VALUE;
    }
    return start + max - System.currentTimeMillis();
  }
  
  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext rfc = (RegionFunctionContext) context;
    PartitionedRegion pr = (PartitionedRegion) rfc.getDataSet();
    
    HDFSFlushQueueArgs args = (HDFSFlushQueueArgs) rfc.getArguments();
    Set<Integer> buckets = new HashSet<Integer>(args.getBuckets());
    buckets.retainAll(pr.getDataStore().getAllLocalPrimaryBucketIds());

    Map<Integer, AsyncFlushResult> flushes = new HashMap<Integer, AsyncFlushResult>();
    for (int bucketId : buckets) {
      try {
        HDFSBucketRegionQueue brq = getQueue(pr, bucketId);
        if (brq != null) {
          if (logger.isDebugEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Flushing bucket " + bucketId)); 
          }
          flushes.put(bucketId, brq.flush());
        }
      } catch (ForceReattemptException e) {
        if (logger.isDebugEnabled() || VERBOSE) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Encountered error flushing bucket " + bucketId), e); 
        }
      }
    }
    
    try {
      long start = System.currentTimeMillis();
      for (Map.Entry<Integer, AsyncFlushResult> flush : flushes.entrySet()) {
        long remaining = waitTime(start, args.getMaxWaitTime());
        if (logger.isDebugEnabled() || VERBOSE) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Waiting for bucket " + flush.getKey() 
              + " to complete flushing, remaining = " + remaining)); 
        }
        
        if (flush.getValue().waitForFlush(remaining, TimeUnit.MILLISECONDS)) {
          if (logger.isDebugEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Bucket " + flush.getKey() + " flushed successfully")); 
          }
          rfc.getResultSender().sendResult(new FlushStatus(flush.getKey()));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    
    if (logger.isDebugEnabled() || VERBOSE) {
      logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Sending final flush result")); 
    }
    rfc.getResultSender().lastResult(FlushStatus.last());
  }

  private HDFSBucketRegionQueue getQueue(PartitionedRegion pr, int bucketId) 
      throws ForceReattemptException {
    AsyncEventQueueImpl aeq = pr.getHDFSEventQueue();
    AbstractGatewaySender gw = (AbstractGatewaySender) aeq.getSender();
    AbstractGatewaySenderEventProcessor ep = gw.getEventProcessor();
    if (ep == null) {
      return null;
    }
    
    ConcurrentParallelGatewaySenderQueue queue = (ConcurrentParallelGatewaySenderQueue) ep.getQueue();
    return queue.getBucketRegionQueue(pr, bucketId);
  }
  
  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }
  
  public static class HDFSFlushQueueResultCollector implements LocalResultCollector<Object, Boolean> {
    private final CountDownLatch complete;
    private final Set<Integer> expectedBuckets;
    private final Set<Integer> successfulBuckets;

    private volatile ReplyProcessor21 processor;
    
    public HDFSFlushQueueResultCollector(Set<Integer> expectedBuckets) {
      this.expectedBuckets = expectedBuckets;
      
      complete = new CountDownLatch(1);
      successfulBuckets = new HashSet<Integer>();
    }
    
    public Set<Integer> getSuccessfulBuckets() {
      synchronized (successfulBuckets) {
        return new HashSet<Integer>(successfulBuckets);
      }
    }
    
    @Override
    public Boolean getResult() throws FunctionException {
      try {
        complete.await();
        synchronized (successfulBuckets) {
          LogWriterI18n logger = InternalDistributedSystem.getLoggerI18n();
          if (logger.fineEnabled() || VERBOSE) {
            logger.info(LocalizedStrings.DEBUG, "Expected buckets: " + expectedBuckets);
            logger.info(LocalizedStrings.DEBUG, "Successful buckets: " + successfulBuckets);
          }
          return expectedBuckets.equals(successfulBuckets);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        GemFireCacheImpl.getExisting().getCancelCriterion().checkCancelInProgress(e);
        throw new FunctionException(e);
      }
    }

    @Override
    public Boolean getResult(long timeout, TimeUnit unit)
        throws FunctionException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void addResult(DistributedMember memberID, Object result) {
      if (result instanceof FlushStatus) {
        FlushStatus status = (FlushStatus) result;
        if (!status.isLast()) {
          synchronized (successfulBuckets) {
            successfulBuckets.add(status.getBucketId());
          }        
        }
      }
    }

    @Override
    public void endResults() {    	
      complete.countDown();
    }

    @Override
    public void clearResults() {
    }

    @Override
    public void setProcessor(ReplyProcessor21 processor) {
      this.processor = processor;
    }

    @Override
    public ReplyProcessor21 getProcessor() {
      return processor;
    }

	@Override
	public void setException(Throwable exception) {
		// TODO Auto-generated method stub
		
	}

  }
}
