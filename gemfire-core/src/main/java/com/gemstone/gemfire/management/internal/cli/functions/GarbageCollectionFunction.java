/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author apande
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


