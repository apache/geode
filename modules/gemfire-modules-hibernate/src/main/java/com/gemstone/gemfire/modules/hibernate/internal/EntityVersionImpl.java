/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.hibernate.internal;

/**
 * 
 * @author sbawaska
 */
public class EntityVersionImpl implements EntityVersion {

  private final Long version;

  public EntityVersionImpl(Long version) {
    this.version = version;
  }

  @Override
  public Long getVersion() {
    return this.version;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EntityVersionImpl) {
      EntityVersionImpl other = (EntityVersionImpl)obj;
      if (this.version.equals(other.version)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.version.hashCode();
  }
}
