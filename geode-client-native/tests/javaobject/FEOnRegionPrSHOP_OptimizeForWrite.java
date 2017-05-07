/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.cache.execute.PartitionedRegionFunctionResultSender;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
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
