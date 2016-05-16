/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.CacheObserverHolder;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy.TestHook;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * An Async event queue listener that writes all of the
 * events in batches to Lucene
 */
public class LuceneEventListener implements AsyncEventListener {
  Logger logger = LogService.getLogger();

  private final RepositoryManager repositoryManager;
  
  public LuceneEventListener(RepositoryManager repositoryManager) {
    this.repositoryManager = repositoryManager;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean processEvents(List<AsyncEvent> events) {
    // Try to get a PDX instance if possible, rather than a deserialized object
    DefaultQuery.setPdxReadSerialized(true);

    Set<IndexRepository> affectedRepos = new HashSet<IndexRepository>();
    
    try {
      for (AsyncEvent event : events) {
        Region region = event.getRegion();
        Object key = event.getKey();
        Object callbackArgument = event.getCallbackArgument();
        
        IndexRepository repository = repositoryManager.getRepository(region, key, callbackArgument);

        Operation op = event.getOperation();
        
        if (testHook != null) {
          testHook.doTestHook("FOUND_AND_BEFORE_PROCESSING_A_EVENT");
        }

        if (op.isCreate()) {
          repository.create(key, event.getDeserializedValue());
        } else if (op.isUpdate()) {
          repository.update(key, event.getDeserializedValue());
        } else if (op.isDestroy()) {
          repository.delete(key);
        } else if (op.isInvalidate()) {
          repository.delete(key);
        } else {
          throw new InternalGemFireError("Unhandled operation " + op + " on " + event.getRegion());
        }
        affectedRepos.add(repository);
      }
      
      for(IndexRepository repo : affectedRepos) {
        repo.commit();
      }
      return true;
    } catch(IOException | BucketNotFoundException e) {
      logger.error("Unable to save to lucene index", e);
      return false;
    } finally {
      DefaultQuery.setPdxReadSerialized(false);
    }
  }
  
  public interface TestHook {
    public void doTestHook(String spot);
  }
  public static TestHook testHook;
}
