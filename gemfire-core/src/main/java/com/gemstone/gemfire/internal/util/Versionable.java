/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

/**
 * 
 * A simple interface that describes the contract of an
 * object that has a version
 * 
 * @author Mitch Thomas
 * @since 5.0
 */
public interface Versionable
{

  public abstract Comparable getVersion();

  public abstract boolean isNewerThan(Versionable other);
  
  public abstract boolean isSame(Versionable other);

  public abstract boolean isOlderThan(Versionable other);
}
