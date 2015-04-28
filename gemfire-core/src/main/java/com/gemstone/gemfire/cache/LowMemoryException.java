/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Indicates a low memory condition either on the local or a remote {@link Cache}.
 * The {@link ResourceManager} monitors local tenured memory consumption and determines when operations are rejected.
 * 
 * @see ResourceManager#setCriticalHeapPercentage(float)
 * @see Region#put(Object, Object)
 * 
 * @author sbawaska
 * @since 6.0
 */
public class LowMemoryException extends ResourceException {

  private static final long serialVersionUID = 6585765466722883168L;
  private final Set<DistributedMember> critMems;

  /**
   * Creates a new instance of <code>LowMemoryException</code>.
   */
  public LowMemoryException() {
    this.critMems = Collections.emptySet();
  }

  /**
   * Constructs an instance of <code>LowMemoryException</code> with the specified detail message.
   * @param msg the detail message
   * @param criticalMembers the member(s) which are/were in a critical state
   */
  public LowMemoryException(String msg, final Set<DistributedMember> criticalMembers) {
    super(msg);
    this.critMems = Collections.unmodifiableSet(criticalMembers);
  }

  /**
   * Get a read-only set of members in a critical state at the time this
   * exception was constructed.
   * @return the critical members
   */
  public Set<DistributedMember> getCriticalMembers() {
    return this.critMems;
  }
}
