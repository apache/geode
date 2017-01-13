/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;
import java.util.*;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.cache.*;

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
