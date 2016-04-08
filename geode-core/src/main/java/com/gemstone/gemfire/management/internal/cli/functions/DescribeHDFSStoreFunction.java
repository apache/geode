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
package com.gemstone.gemfire.management.internal.cli.functions;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.util.HDFSStoreNotFoundException;

/**
 *  Function used by the 'describe hdfs-store' gfsh command to collect information
 * and details about a particular hdfs store for a particular GemFire distributed system member.
 * 
 */
public class DescribeHDFSStoreFunction extends FunctionAdapter implements InternalEntity {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();

  public static DescribeHDFSStoreFunction INSTANCE = new DescribeHDFSStoreFunction();

  private static final String ID = DescribeHDFSStoreFunction.class.getName();
  
  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }
  
  protected DistributedMember getDistributedMemberId(Cache cache){
    return ((InternalCache)cache).getMyId();
  }
  
  public void execute(final FunctionContext context) {
    try {
      Cache cache = getCache();
      final DistributedMember member = getDistributedMemberId(cache);      
      if (cache instanceof GemFireCacheImpl) {
        GemFireCacheImpl cacheImpl = (GemFireCacheImpl)cache;
        final String hdfsStoreName = (String)context.getArguments();
        final String memberName = member.getName();
        HDFSStoreImpl hdfsStore = cacheImpl.findHDFSStore(hdfsStoreName);        
        if (hdfsStore != null) {
          HDFSStoreConfigHolder configHolder = new HDFSStoreConfigHolder (hdfsStore);
          context.getResultSender().lastResult(configHolder);
        }
        else {
          context.getResultSender().sendException(
              new HDFSStoreNotFoundException(
                  String.format("A hdfs store with name (%1$s) was not found on member (%2$s).",
                  hdfsStoreName, memberName)));
        }
      }  
    } catch (Exception e) {
      logger.error("Error occurred while executing 'describe hdfs-store': {}!", e.getMessage(), e);
      context.getResultSender().sendException(e);
    }
  }

  @Override
  public String getId() {
    return ID;
  }	
}
