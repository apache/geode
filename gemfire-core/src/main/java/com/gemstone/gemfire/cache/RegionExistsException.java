/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * Indicates that the requested region already exists when a region is
 * being created.
 *
 * @author Eric Zoerner
 *
 * 
 * @see Region#createSubregion
 * @see Cache#createRegion
 * @since 2.0
 */
public class RegionExistsException extends CacheException {
private static final long serialVersionUID = -5643670216230359426L;
  private transient Region region;
  
  /**
   * Constructs an instance of <code>RegionExistsException</code> with the specified Region.
   * @param rgn the Region that exists
   */
  public RegionExistsException(Region rgn) {
    super(rgn.getFullPath());
    this.region = rgn;
  }
  
  /**
   * Constructs an instance of <code>RegionExistsException</code> with the specified detail message
   * and cause.
   * @param rgn the Region that exists
   * @param cause the causal Throwable
   */
  public RegionExistsException(Region rgn, Throwable cause) {
    super(rgn.getFullPath(), cause);
    this.region = rgn;
  }
  
  
  /**
   * Return the Region that already exists which prevented region creation.
   * @return the Region that already exists, or null if this exception has
   * been serialized, in which {@link Throwable#getMessage } will return the
   * pathFromRoot for the region that exists.
   */
  public Region getRegion() {
    return this.region;
  }
}
