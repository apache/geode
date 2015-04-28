/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.persistence;

import java.net.InetAddress;
import java.util.UUID;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.cache.DataPolicy;

/**
 * A pattern describing a single member's a set of persistent files for a region.
 * When a member has a region defined with the a data policy of
 * {@link DataPolicy#PERSISTENT_REPLICATE}, that members persistent files are
 * assigned a unique ID. After a failure of all members, during recovery, the
 * persistent members will wait for all persistent copies of the region to be
 * recovered before completing region initialization.
 * 
 * This pattern describes what unique ids the currently recovering persistent
 * members are waiting for. See
 * {@link AdminDistributedSystem#getMissingPersistentMembers()}
 * 
 * @author dsmith
 * @since 6.5
 *
 */
public interface PersistentID extends DataSerializable {

  /**
   * The host on which the persistent data was last residing
   */
  public abstract InetAddress getHost();

  /**
   * The directory which the persistent data was last residing in.
   */
  public abstract String getDirectory();
  
  /**
   * The unique identifier for the persistent data.
   * @since 7.0
   */
  public abstract UUID getUUID();
}