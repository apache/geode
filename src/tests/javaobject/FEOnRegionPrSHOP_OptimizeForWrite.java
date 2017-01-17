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
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import java.util.ArrayList;
import java.util.Properties;
import java.util.*;

public class FEOnRegionPrSHOP_OptimizeForWrite extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext context) {   
    RegionFunctionContext regionContext = (RegionFunctionContext)context;
    PartitionedRegionFunctionResultSender rs = (PartitionedRegionFunctionResultSender)regionContext.getResultSender();	
	ResultSender sender = context.getResultSender();	
    PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
	
	// Use this for debugging if bucketIds dont match.
	/*Set keys = regionContext.getFilter();
    System.out.println("Context " + keys);
    ArrayList vals = new ArrayList();
    Iterator itr = keys.iterator();
    while (itr.hasNext()) {
      Object k = itr.next();	  
	  Integer bucketId = new Integer(PartitionedRegionHelper.getHashKey(pr, k));    
  	  System.out.println("CPPTEST: found BucketId: " + bucketId);    
  	  List bucketListOnNode = pr.getLocalPrimaryBucketsListTestOnly();    
  	  System.out.println("CPPTEST: bucketListOnNode size is: " + bucketListOnNode.size());
	  Iterator it=bucketListOnNode.iterator();
      while(it.hasNext())
      {
        Integer value=(Integer)it.next();
        System.out.println("Value :"+value);
      }
  	  if (!bucketListOnNode.contains(bucketId)) {
 		System.out.println("CPPTEST: Wrong BucketId from list");
		sender.lastResult(false);
		return;		  
  	  }	  
	  if (PartitionRegionHelper.getPrimaryMemberForKey(pr, k) == null) {
      System.out.println("CPPTEST: Wrong BucketId from member");
	  sender.lastResult(false);
	  return;  	      
      }      
    }*/	
    sender.lastResult(rs.isLocallyExecuted());	 //This is single hop indication.	
  }

  public String getId() {
    return "FEOnRegionPrSHOP_OptimizeForWrite";
  }
  
  public boolean optimizeForWrite() {
    return true;
  }

  public void init(Properties arg0) {
  }
}
