/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

/**
 * A snapshot ("segment") of a entity (either a {@linkplain
 * RegionSnapshot region} or an {@linkplain EntrySnapshot entry} in a
 * cache.
 */
public interface CacheSnapshot extends java.io.Serializable {
  public Object getName();
  public Object getUserAttribute();

  public long getLastModifiedTime();
  public long getLastAccessTime();
  public long getNumberOfHits();
  public long getNumberOfMisses();
  public float getHitRatio();
}
