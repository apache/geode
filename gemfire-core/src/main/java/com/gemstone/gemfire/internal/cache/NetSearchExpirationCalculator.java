/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;

/**
 * EntryExpiryTask already implements the algorithm for figuring out
 * expiration.  This class has it use the remote expiration attributes 
 * provided by the netsearch, rather than this region's attributes.
 */
public class NetSearchExpirationCalculator extends EntryExpiryTask {
  
  private final ExpirationAttributes idleAttr;
  private final ExpirationAttributes ttlAttr;
  
  /** Creates a new instance of NetSearchExpirationCalculator */
  public NetSearchExpirationCalculator(LocalRegion region, Object key, int ttl, int idleTime) {
    super(region, null);
    idleAttr = new ExpirationAttributes(idleTime, ExpirationAction.INVALIDATE);
    ttlAttr = new ExpirationAttributes(ttl, ExpirationAction.INVALIDATE); 
  }

  @Override
  protected ExpirationAttributes getTTLAttributes() {
    return this.ttlAttr;
  }

  @Override
  protected ExpirationAttributes getIdleAttributes() {
    return this.idleAttr;
  }
  
}
