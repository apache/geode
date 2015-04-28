package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.ExpirationAttributes;

/**
 * Remembers the expiration attributes returned from
 * the customer's CustomExpiry callback, if any.
 * 
 * @author dschneider
 * @since 8.0
 *
 */
public class CustomEntryExpiryTask extends EntryExpiryTask {  
  private final ExpirationAttributes ttlAttr;
  private final ExpirationAttributes idleAttr;

  public CustomEntryExpiryTask(LocalRegion region, RegionEntry re, ExpirationAttributes ttlAtts, ExpirationAttributes idleAtts) {
    super(region, re);
    this.ttlAttr = ttlAtts;
    this.idleAttr = idleAtts;
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
