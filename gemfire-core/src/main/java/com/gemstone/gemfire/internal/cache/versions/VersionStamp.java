/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.versions;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * @author bruce
 *
 */
public interface VersionStamp<T extends VersionSource> extends VersionHolder<T> {

  
  
  /**
   * set the time stamp from the given clock value
   */
  void setVersionTimeStamp(long time);
  
//  /**
//   * @return the ID of the previous member that last changed the corresponding entry.
//   */
//  DistributedMember getPreviousMemberID();


  /**
   * Sets the version information with what is in the tag
   * 
   * @param tag the entryVersion to set
   */
  void setVersions(VersionTag<T> tag);

  /**
   * @param memberID the memberID to set
   */
  void setMemberID(VersionSource memberID);

//  /**
//   * @param previousMemberID the previousMemberID to set
//   */
//  void setPreviousMemberID(DistributedMember previousMemberID);

  /**
   * returns a VersionTag carrying this stamps information.  This is used
   * for transmission of the stamp in initial image transfer
   */
  VersionTag<T> asVersionTag();


  /**
   * Perform a versioning check with the incoming event.  Throws a
   * ConcurrentCacheModificationException if there is a problem.
   * @param event
   */
  public void processVersionTag(EntryEvent event);
  
  /**
   * Perform a versioning check with the given GII information.  Throws a
   * ConcurrentCacheModificationException if there is a problem.
   * @param r the region being modified
   * @param tag the version info for the modification
   * @param isTombstoneFromGII it's a tombstone
   * @param hasDelta it has delta
   * @param thisVM this cache's DM identifier
   * @param sender the identifier of the member providing the entry
   * @param checkConflicts true if conflict checks should be performed
   */
  public void processVersionTag(LocalRegion r, VersionTag<T> tag, boolean isTombstoneFromGII,
      boolean hasDelta,
      VersionSource thisVM, InternalDistributedMember sender, boolean checkConflicts);
  
  /**
   * return true if this stamp has valid entry/region version information, false if not
   */
  public boolean hasValidVersion();
}
