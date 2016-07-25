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

import java.util.HashMap;
import java.util.Map;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.CliUtil;

/**
 * 
 * Class for Garbage collection function
 * 
 *  
 * 
 */
public class GarbageCollectionFunction implements Function, InternalEntity {
 public static final String ID = GarbageCollectionFunction.class.getName();
  

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    StringBuilder str1 = new StringBuilder();
    Map<String, String> resultMap = null;
    try{     
        Cache cache = CacheFactory.getAnyInstance();        
        DistributedMember member = cache.getDistributedSystem().getDistributedMember();
        long freeMemoeryBeforeGC = Runtime.getRuntime().freeMemory();
        long totalMemoryBeforeGC = Runtime.getRuntime().totalMemory();
        long timeBeforeGC = System.currentTimeMillis();
        Runtime.getRuntime().gc(); 
       
        long freeMemoeryAfterGC = Runtime.getRuntime().freeMemory();
        long totalMemoryAfterGC = Runtime.getRuntime().totalMemory();
        long timeAfterGC = System.currentTimeMillis() ;
        
        long megaBytes = 131072;
        resultMap = new HashMap<String,String>();
        resultMap.put("MemberId", member.getId());
        resultMap.put("HeapSizeBeforeGC", String.valueOf((totalMemoryBeforeGC - freeMemoeryBeforeGC) / megaBytes));
        resultMap.put("HeapSizeAfterGC",String.valueOf( (totalMemoryAfterGC - freeMemoeryAfterGC)/megaBytes));
        resultMap.put("TimeSpentInGC", String.valueOf(timeAfterGC - timeBeforeGC));      
    }catch(Exception ex){
      str1.append("Exception in GC:"+ ex.getMessage() + CliUtil.stackTraceAsString((Throwable)ex));
      context.getResultSender().lastResult(str1.toString());    
    }        
    context.getResultSender().lastResult(resultMap);    
  }
  @Override
  public String getId() {
    return GarbageCollectionFunction.ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    //no need of optimization since read-only.
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}


