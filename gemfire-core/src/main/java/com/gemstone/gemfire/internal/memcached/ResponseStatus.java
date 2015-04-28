/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached;

/**
 * encapsulate ResponseOpCodes for binary reply messages.
 * 
 * @author Swapnil Bawaskar
 */
public enum ResponseStatus {

  NO_ERROR {
    @Override
    public short asShort() {
      return 0x0000;
    }
  },
  KEY_NOT_FOUND {
    @Override
    public short asShort() {
      return 0x0001;
    }
  },
  KEY_EXISTS {
    @Override
    public short asShort() {
      return 0x0002;
    }
  },
  ITEM_NOT_STORED {
    @Override
    public short asShort() {
      return 0x0005;
    }
  },
  NOT_SUPPORTED {
    @Override
    public short asShort() {
      return 0x0083;
    }
  },
  INTERNAL_ERROR {
    @Override
    public short asShort() {
      return 0x0084;
    }
  };
  
  public abstract short asShort();
}
