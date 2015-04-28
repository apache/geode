/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * This class manages in memory copy of the canonical ids held in the disk
 * init file. It's used by the init file to assign numbers to new ids and 
 * retrieve existing ids.
 * 
 * This class is not thread safe, so it should be synchronized externally.
 * 
 * @author dsmith
 *
 */
public class CanonicalIdHolder {
  private static final Logger logger = LogService.getLogger();

  /**
   * Map of integer representation to canonicalized member ids.
   */
  private Int2ObjectOpenHashMap idToObject = new Int2ObjectOpenHashMap();
  
  /**
   * Map of canonicalized member ids to integer representation.
   */
  private Object2IntOpenHashMap objectToID = new Object2IntOpenHashMap();
  
  private int highestID = 0;
  
  /**
   * Add a mapping that we have recovered from disk
   */
  public void addMapping(int id, Object object) {
    //Store the mapping
    idToObject.put(id, object);
    objectToID.put(object, id);
    
    //increase the next canonical id the recovered id is higher than it.
    highestID = highestID  < id ? id : highestID;
  }
  
  /**
   * Get the id for a given object 
   */
  public int getId(Object object) {
    return objectToID.getInt(object);
  }
  
  /**
   * Get the object for a given id.
   */
  public Object getObject(int id) {
    return idToObject.get(id);
  }
  
  /**
   * Create an id of the given object.
   * @param object
   * @return the id generated for this object.
   */
  public int createId(Object object) {
    assert !objectToID.containsKey(object);
    int id = ++highestID;
    objectToID.put(object, id);
    idToObject.put(id, object);
    return id;
  }
  
  /**
   * Get all of the objects that are mapped.
   * @return a map of id to object for all objects
   * held by this canonical id holder.
   */
  public Int2ObjectOpenHashMap getAllMappings() {
    return idToObject;
  }

  

}
