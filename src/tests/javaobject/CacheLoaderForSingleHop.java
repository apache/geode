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
