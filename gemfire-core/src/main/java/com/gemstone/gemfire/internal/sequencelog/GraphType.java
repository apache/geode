/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog;

import java.util.EnumSet;

/**
 * @author dsmith
 *
 */
public enum GraphType {
  REGION,
  KEY,
  MESSAGE,
  MEMBER;
  
  public byte getId() {
    return (byte) this.ordinal();
  }

  public static GraphType getType(byte id) {
    return values()[id];
  }

  public static EnumSet<GraphType> parse(String enabledTypesString) {
    
    EnumSet<GraphType> set = EnumSet.noneOf(GraphType.class);
    if(enabledTypesString.contains("region")) {
      set.add(REGION);
    }
    if(enabledTypesString.contains("key")) {
      set.add(KEY);
    }
    if(enabledTypesString.contains("message")) {
      set.add(MESSAGE);
    }
    if(enabledTypesString.contains("member")) {
      set.add(MEMBER);
    }
    if(enabledTypesString.contains("all")) {
      set = EnumSet.allOf(GraphType.class);
    }
    
    return set;
  }
}
