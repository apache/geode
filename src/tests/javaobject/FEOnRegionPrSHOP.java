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
package javaobject;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultSender;

import java.util.Properties;

public class FEOnRegionPrSHOP extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext context) {   
    RegionFunctionContext regionContext = (RegionFunctionContext)context;
    PartitionedRegionFunctionResultSender rs = (PartitionedRegionFunctionResultSender)regionContext.getResultSender();	
	ResultSender sender = context.getResultSender();	  
    sender.lastResult(rs.isLocallyExecuted());	 //This is single hop indication.
  }

  public String getId() {
    return "FEOnRegionPrSHOP";
  }
  
  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties arg0) {
  }
}
