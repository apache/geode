/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;
import java.util.*;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.cache.*;

public class CacheLoaderForSingleHop implements Declarable, CacheLoader {

  public CacheLoaderForSingleHop() 
  {
  }
  
  public void init(Properties props) {
  }
  
  public Boolean checkSingleHop(LoaderHelper helper) {    
  
    System.out.println("CPPTEST: java key: " + helper.getKey());
  
    if (helper.getKey() instanceof Integer && ((Integer) helper.getKey()).intValue() < 1500) {
      return null;
    }
    
  	PartitionedRegion pr;
    
  	try {
  	  pr = (PartitionedRegion) helper.getRegion();	  
  	} catch (Exception ex) {
  	  //throw new CacheLoaderException("PartitionRegion Cast failed due to: "+ ex.getMessage());
      return Boolean.FALSE;
  	}
    
  	Integer bucketId = new Integer(PartitionedRegionHelper.getHashKey(pr, helper.getKey()));
    
  	System.out.println("CPPTEST: found BucketId: " + bucketId);
    
  	List bucketListOnNode = pr.getLocalBucketsListTestOnly();
    
  	System.out.println("CPPTEST: bucketListOnNode size is: " + bucketListOnNode.size());
    
  	if (!bucketListOnNode.contains(bucketId)) {
  		System.out.println("CPPTEST: Wrong BucketId from list");
  	  //throw new CacheLoaderException("Calculated bucketId " + bucketId + 
  		  //" does not match java partitionedRegion BucketId" );		
      return Boolean.FALSE;
  	}
    
    if (PartitionRegionHelper.getPrimaryMemberForKey(pr, helper.getKey()) == null) {
      System.out.println("CPPTEST: Wrong BucketId from member");
  	  //throw new CacheLoaderException("Calculated bucketId " + bucketId + 
  		  //" does not match java partitionedRegion BucketId" );		
      return Boolean.FALSE;
    }
    
    System.out.println("CPPTEST: cache loader OK BucketId");
    
    return Boolean.TRUE;
  }

  public Object load(LoaderHelper helper)
  {
    System.out.println("CPPTEST: Inside load of loader");
  
  	try {
  	  return checkSingleHop(helper);
  	}
  	catch (Exception ex) {
  		return Boolean.FALSE;
  	}
  }
   
   public void close()
   {
   }
}
