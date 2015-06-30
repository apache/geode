package com.gemstone.gemfire.modules.hibernate.internal;

import org.hibernate.cache.access.SoftLock;

/**
 * 
 * @author sbawaska
 */
public interface EntityVersion extends SoftLock {

  public Long getVersion();
}
