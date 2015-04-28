/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;

/**
 * Contains common data tranformation utility methods and transformers.
 * @author rholmes
 */
public final class TransformUtils {
  
  /**
   * Transforms PersistentMemberIDs to a user friendly log entry.
   */
  public final static Transformer<Map.Entry<PersistentMemberID, Set<Integer>>, String> persistentMemberEntryToLogEntryTransformer = new Transformer<Map.Entry<PersistentMemberID, Set<Integer>>,String> () {
    @Override
    public String transform(Map.Entry<PersistentMemberID, Set<Integer>> entry) {
      PersistentMemberID memberId = entry.getKey();
      Set<Integer> bucketIds = entry.getValue();
      StringBuilder builder = new StringBuilder();
      builder.append(persistentMemberIdToLogEntryTransformer.transform(memberId));
  
      if(null != bucketIds) {
        builder.append("  Buckets: ");
        builder.append(bucketIds);
      }
        
      builder.append("\n");
      
      return builder.toString();
    }      
  };
  
  /**
   * Transforms PersistentMemberIDs to a user friendly log entry.
   */
  public final static Transformer<PersistentMemberID, String> persistentMemberIdToLogEntryTransformer = new Transformer<PersistentMemberID, String> () {
    @Override
    public String transform(PersistentMemberID memberId) {
      StringBuilder builder = new StringBuilder();
      
      if(null != memberId) {
        if(null != memberId.diskStoreId) {
          builder.append("\n  DiskStore ID: ");
          builder.append(memberId.diskStoreId.toUUID().toString());        
        }
        
        if(null != memberId.name) {
          builder.append("\n  Name: ");
          builder.append(memberId.name);
        }
  
        if((null != memberId.host) && (null != memberId.directory)) {
          builder.append("\n  Location: ");          
        }
        
        if(null != memberId.host) {
          builder.append("/");
          builder.append(memberId.host.getHostAddress());
          builder.append(":");        
        }
        
        if(null != memberId.directory) {
          builder.append(memberId.directory);        
        }
        
        builder.append("\n");
      }
      
      return builder.toString();
    }      
  };

  /**
   * This is a simple file to file name transformer.
   */
  public final static Transformer<File,String> fileNameTransformer = new Transformer<File,String>() {
    public String transform(File file) {
      return file.getName();
    }
  };
  
  /**
   * Transforms a collection of one data type into another.
   * @param from a collection of data to be transformed.
   * @param to a collection to contain the transformed data.
   * @param transformer transforms the data.
   */
  public static <T1, T2> void transform(Collection<T1> from,Collection<T2> to,Transformer<T1,T2> transformer) {
    for(T1 instance : from) {
      to.add(transformer.transform(instance));
    }
  }

  /**
   * Transforms a collection of one data type into another and returns a map
   * using the transformed type as the key and the original type as the value.
   * @param from a collection of data to be transformed.
   * @param transformer transforms the data.
   * 
   * @return a Map of transformed values that are keys to the original values.
   */
  public static <T1, T2> Map<T2, T1> transformAndMap(Collection<T1> from,Transformer<T1,T2> transformer) {
    Map<T2,T1> map = new HashMap<T2,T1>();
    for(T1 instance : from) {
      map.put(transformer.transform(instance), instance);
    }
    
    return map;
  }
}
