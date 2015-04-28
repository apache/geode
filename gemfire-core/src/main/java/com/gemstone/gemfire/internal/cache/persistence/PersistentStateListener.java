/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public interface PersistentStateListener {
  public static class PersistentStateAdapter implements PersistentStateListener {
  
    public void memberOffline(InternalDistributedMember member,
        PersistentMemberID persistentID) {
    }
  
    public void memberOnline(InternalDistributedMember member,
        PersistentMemberID persistentID) {
    }
  
    public void memberRemoved(PersistentMemberID persistentID, boolean revoked) {
    }
  }
  public void memberOnline(InternalDistributedMember member, PersistentMemberID persistentID);
  public void memberOffline(InternalDistributedMember member, PersistentMemberID persistentID);
  public void memberRemoved(PersistentMemberID persistentID, boolean revoked);
}