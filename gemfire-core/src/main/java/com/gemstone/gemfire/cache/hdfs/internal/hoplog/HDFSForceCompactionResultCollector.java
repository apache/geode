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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.execute.LocalResultCollector;

/**
 * 
 * @author sbawaska
 */
public class HDFSForceCompactionResultCollector implements LocalResultCollector<Object, List<CompactionStatus>> {

  /** list of received replies*/
  private List<CompactionStatus> reply = new ArrayList<CompactionStatus>();

  /** semaphore to block the caller of getResult()*/
  private CountDownLatch waitForResults = new CountDownLatch(1);

  /** boolean to indicate if clearResults() was called to indicate a failure*/
  private volatile boolean shouldRetry;

  private ReplyProcessor21 processor;

  @Override
  public List<CompactionStatus> getResult() throws FunctionException {
    try {
      waitForResults.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      GemFireCacheImpl.getExisting().getCancelCriterion().checkCancelInProgress(e);
      throw new FunctionException(e);
    }
    return reply;
  }

  @Override
  public List<CompactionStatus> getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResult(DistributedMember memberID,
      Object resultOfSingleExecution) {
    if (resultOfSingleExecution instanceof CompactionStatus) {
      CompactionStatus status = (CompactionStatus) resultOfSingleExecution;
      if (status.getBucketId() != HDFSForceCompactionFunction.BUCKET_ID_FOR_LAST_RESULT) {
        reply.add(status);
      }
    }
  }

  @Override
  public void endResults() {
    waitForResults.countDown();
  }

  @Override
  public void clearResults() {
    this.shouldRetry = true;
    waitForResults.countDown();
  }

  /**
   * @return true if retry should be attempted
   */
  public boolean shouldRetry() {
    return this.shouldRetry || !getFailedBucketIds().isEmpty();
  }

  private Set<Integer> getFailedBucketIds() {
    Set<Integer> result = new HashSet<Integer>();
    for (CompactionStatus status : reply) {
      if (!status.isStatus()) {
        result.add(status.getBucketId());
      }
    }
    return result;
  }

  public Set<Integer> getSuccessfulBucketIds() {
    Set<Integer> result = new HashSet<Integer>();
    for (CompactionStatus status : reply) {
      if (status.isStatus()) {
        result.add(status.getBucketId());
      }
    }
    return result;
  }

  @Override
  public void setProcessor(ReplyProcessor21 processor) {
    this.processor = processor;
  }

  @Override
  public ReplyProcessor21 getProcessor() {
    return this.processor;
  }

@Override
public void setException(Throwable exception) {
	// TODO Auto-generated method stub
	
}
}

