/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author ajayp
 * @since 8.0
 */

public class MembersForRegionFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = MembersForRegionFunction.class.getName();

  @Override
  public void execute(FunctionContext context) {
    Map<String, String> resultMap = new HashMap<String,String>();
    try{
      Cache  cache          = CacheFactory.getAnyInstance();      
      String memberNameOrId = cache.getDistributedSystem().getDistributedMember().getId();  
      Object args = (Object) context.getArguments();
      String regionName = ((String) args );
      Region<Object, Object> region = cache.getRegion(regionName);           
      
      if(region != null){
        resultMap.put(memberNameOrId, "" + region.getAttributes().getScope().isLocal());        
       }else{
         String regionWithPrefix  = Region.SEPARATOR + regionName ; 
         region = cache.getRegion(regionWithPrefix);   
         if(region != null){           
           resultMap.put(memberNameOrId, "" + region.getAttributes().getScope().isLocal());
         }else{           
           resultMap.put("", "" );
         }                  
      }
      context.getResultSender().lastResult( resultMap);        
    }catch(Exception ex){
      Cache  cache          = CacheFactory.getAnyInstance();     
      logger.info("MembersForRegionFunction exception {}", ex.getMessage(), ex);
      resultMap.put("", "" );
      context.getResultSender().lastResult( resultMap);
    }    
  }

  @Override
  public String getId() {
    return MembersForRegionFunction.ID;
  }
  
  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

}
