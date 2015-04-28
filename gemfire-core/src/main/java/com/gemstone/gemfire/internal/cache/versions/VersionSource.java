/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * The member that originated an update that is stored in the version
 * information of a region entry, or in the regions version vector.
 * 
 * A VersionMember could either be an InternalDistributedMember (for an in
 * memory region), or a UUID (for a persistent region).
 * 
 * VersionMembers should implement equals and hashcode.
 * 
 * @author dsmith
 *
 */
public interface VersionSource<T> extends DataSerializableFixedID, Comparable<T> {
  
  public void writeEssentialData(DataOutput out) throws IOException;
  
}
