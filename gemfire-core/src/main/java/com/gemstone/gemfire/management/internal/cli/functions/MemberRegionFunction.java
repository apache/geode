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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.InternalEntity;


public class MemberRegionFunction implements Function, InternalEntity {
  public static final String ID = MemberRegionFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String region = (String) args[0];
    String functionId = (String) args[1];    
    Cache cache = CacheFactory.getAnyInstance();    
    
    try {      
      Function function = FunctionService.getFunction(functionId);
      if (function == null){
        context.getResultSender().lastResult("For region on a member did not get function "+functionId);
      }
      Execution execution = FunctionService.onRegion(cache.getRegion(region));
      if (execution == null){
        context.getResultSender().lastResult("For region on a member could not execute");
      }else{
        execution.execute(function);
        context.getResultSender().lastResult("succeeded in executing on region "+region);
      }

    }catch(FunctionException e){
      context.getResultSender().lastResult(
          "FunctionException in MemberRegionFunction =" + e.getMessage());
    }catch (Exception e) {
      context.getResultSender().lastResult(
          "Exception in MemberRegionFunction =" + e.getMessage());
    }
    
  }

  @Override
  public String getId() {
    return MemberRegionFunction.ID;

  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    // no need of optimization since read-only.
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

}
