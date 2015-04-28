/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * 
 * @author russell
 */
public class VersionTimestamp
  {

  /** actual storage of version timestamp is in a scalar long value */
  long version = 0;

  /** Creates a new instance of VersionTimestamp */
  public VersionTimestamp() {
  }

  public void increment()
  {
    version++;
  }

  public long getVersion()
  {
    return version;
  }

  public void setVersion(long newVersion)
  {
    version = newVersion;
  }
}
