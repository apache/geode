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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CancellationException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.logging.LogService;



public class RebalanceFunction implements Function, InternalEntity{
  private static final Logger logger = LogService.getLogger();
  
  public static final String ID = RebalanceFunction.class.getName();
  

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {   
    
    RebalanceOperation op = null;
    String[] str = new String[0];

    Cache cache = CacheFactory.getAnyInstance();    
    ResourceManager manager = cache.getResourceManager();
    Object[] args = (Object[]) context.getArguments();
    String simulate = ((String) args[0]);
    Set<String> includeRegionNames = (Set<String> ) args[1]; 
    Set<String> excludeRegionNames = (Set<String> ) args[2]; 
    RebalanceFactory rbFactory =manager.createRebalanceFactory();    
    rbFactory.excludeRegions(excludeRegionNames);
    rbFactory.includeRegions(includeRegionNames);
    RebalanceResults results = null;
    
    if (simulate.equals("true")){
      op = rbFactory.simulate();  
    }else{
      op = rbFactory.start();  
    }
      
    try {
      results = op.getResults();      
      logger.info("Starting RebalanceFunction got results = {}", results);
      StringBuilder str1 = new StringBuilder();      
      str1.append(results.getTotalBucketCreateBytes() + "," +
      results.getTotalBucketCreateTime() + "," +
      results.getTotalBucketCreatesCompleted() + "," +
      results.getTotalBucketTransferBytes() + "," +
      results.getTotalBucketTransferTime() + "," +
      results.getTotalBucketTransfersCompleted() + "," +
      results.getTotalPrimaryTransferTime() + "," +
      results.getTotalPrimaryTransfersCompleted() + "," +
      results.getTotalTime()+ ","  );
      
      Set<PartitionRebalanceInfo> regns1 = results.getPartitionRebalanceDetails();
      Iterator it = regns1.iterator();      
      while (it.hasNext()) {
        PartitionRebalanceInfo rgn = (PartitionRebalanceInfo) it.next();        
        str1.append(rgn.getRegionPath() + ",");
      }      
      logger.info("Starting RebalanceFunction str1={}", str1);
      context.getResultSender().lastResult(str1.toString());      
    } catch (CancellationException e) {
      logger.info("Starting RebalanceFunction CancellationException: ", e.getMessage(), e);
      context.getResultSender().lastResult(
          "CancellationException1 " + e.getMessage());
    } catch (InterruptedException e) {      
      logger.info("Starting RebalanceFunction InterruptedException: {}", e.getMessage(), e);
      context.getResultSender().lastResult(
          "InterruptedException2 " + e.getMessage());
    }  
  }

  @Override
  public String getId() {
    return RebalanceFunction.ID;
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
