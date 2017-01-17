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
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
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
import java.util.List;

public class CacheWriterForSingleHop<K,V> implements Declarable, CacheWriter<K,V> {

  public CacheWriterForSingleHop() 
  {
  }
  
  public void init(Properties props) {
  }
  
  public void checkSingleHop(EntryEvent<K,V> event) {
	  checkSingleHop(event, true);
  }
  
  public void checkSingleHop(EntryEvent<K,V> event, boolean checkhash) {	

    System.out.println("CPPTEST: java key: " + event.getKey());
    System.out.println("CPPTEST: java newvalue: " + event.getNewValue());
    System.out.println("CPPTEST: java hashcode: " + event.getKey().hashCode());  
    System.out.println("CPPTEST: operation: " + event.getOperation());  
    
    
    if ((event.getNewValue() instanceof Boolean) || (event.getNewValue() instanceof String)) {
      System.out.println("CPPTEST: Short-circuiting CacheWriter because its a special put.");
      return;
    }
    
    /*
    if (event.getOperation() == Operation.LOCAL_LOAD_CREATE) {
      System.out.println("CPPTEST: Detected NWHOP for LOCAL_LOAD_CREATE.");
      return;
    }
    */
    
    if (event.isOriginRemote()) {
      System.out.println("CPPTEST: Detected NWHOP via isOriginRemote() for operation " + event.getOperation());
      return;
    }
    
    if (checkhash) {
    
      int hashcode = event.getKey().hashCode();
      
      int value = Integer.parseInt( event.getNewValue().toString() );
      
      if (event.getKey().hashCode() != value){
        System.out.println("CPPTEST: hashcode did not match value");
        throw new CacheWriterException("java value " + event.getNewValue().toString() + 
        	  " does not match " + event.getKey().hashCode() + " java hashcode" );		 
        //return;
      }
      
      System.out.println("CPPTEST: hashcode check OK");
    }
    
  	PartitionedRegion pr;
    
  	try {
  	  pr = (PartitionedRegion) ((EntryEventImpl)event).getRegion();	  
  	} catch (Exception ex) {
  	  throw new CacheWriterException("PartitionRegion Cast failed due to: "+ ex.getMessage());
      //System.out.println("CPPTEST: PartitionRegion Cast failed due to: "+ ex.getMessage());
      //return;
  	}
    
  	Integer bucketId = new Integer(PartitionedRegionHelper.getHashKey(pr, null, event.getKey(), null, null));
    
  	System.out.println("CPPTEST: found BucketId: " + bucketId);
    
  	List bucketListOnNode = pr.getLocalPrimaryBucketsListTestOnly();
    
  	System.out.println("CPPTEST: bucketListOnNode size is: " + bucketListOnNode.size());
    
    /*
  	if (!bucketListOnNode.contains(bucketId)) {
  		System.out.println("CPPTEST: Wrong BucketId from list");
  	  //throw new CacheWriterException("Calculated bucketId " + bucketId + 
  		  //" does not match java partitionedRegion BucketId" );	
      return;      
  	}
    */
    
    if (PartitionRegionHelper.getPrimaryMemberForKey(pr, event.getKey()) == null) {
      System.out.println("CPPTEST: Wrong BucketId from member");
  	  throw new CacheWriterException("Calculated bucketId " + bucketId + 
  		  " does not match java partitionedRegion BucketId" );		
      //return;
    }
    
	System.out.println("CPPTEST: cache writer OK BucketId");
    
    // We need to put this into the region here because throwing an exception
    // aborts the put thus causing the subsequent destroy op to fail because
    // the CacheWriter itself is not invoked due to EntryNotFoundException.
    
    //pr.putIfAbsent(event.getKey(), "placeholder");

    /*
       We operate backwards, throwing exception for success conditions
       and simply returning for failure conditions due to the way
       PR do not (upon extra hop) invoke writers/loaders on secondaries.
    */
    
    /*throw new CacheWriterException("Calculated bucketId " + bucketId + 
			" matches java partitionedRegion primary BucketId" );*/
  }

  public void beforeCreate(EntryEvent<K,V> event) {
    System.out.println("CPPTEST: Inside beforeCreate");
    checkSingleHop(event);    	         
  }
  
  public void beforeUpdate(EntryEvent<K,V> event) 
  {
    System.out.println("CPPTEST: Inside beforeUpdate");   
    checkSingleHop(event);
  }
   
  public void beforeDestroy(EntryEvent<K,V> event) 
  {
    System.out.println("CPPTEST: Inside beforeDestroy");   
    checkSingleHop(event, false);
  }
   
  public void beforeRegionDestroy(RegionEvent<K,V> event) 
  {
  }

  public void beforeRegionClear(RegionEvent<K,V> event) 
  {
  }

  public void close()
  {
  }
}
