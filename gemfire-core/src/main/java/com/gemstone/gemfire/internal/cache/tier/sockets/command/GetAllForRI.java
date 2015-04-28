/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.ObjectPartList651;
import com.gemstone.gemfire.internal.cache.tier.sockets.SerializedObjectPartList;

/**
 * A version of GetAll which in which the response contains the objects in
 * serialized byte array form, so that they can be separated into individual
 * values without being deserialized. The standard GetAll requires us to
 * deserialize the value of every object.
 * 
 * [bruce] this class is superceded by GetAll70, which merges GetAll651 and
 * GetAllForRI
 * 
 * @author dsmith
 * 
 */
public class GetAllForRI extends GetAll651 {
  private final static GetAllForRI singleton = new GetAllForRI();
  
  public static Command getCommand() {
    return singleton;
  }
  
  protected GetAllForRI() {
  }

  @Override
  protected ObjectPartList651 getObjectPartsList(boolean includeKeys) {
    return new SerializedObjectPartList(maximumChunkSize, includeKeys);
  }
  
  

}
