/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tx;

import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation;
import com.gemstone.gemfire.internal.cache.DistributedRemoveAllOperation;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.KeyInfo;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;

import java.util.Set;

public interface TXRegionStub {

  void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue);

  Entry getEntry(KeyInfo keyInfo, boolean allowTombstone);

  void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry);

  boolean containsKey(KeyInfo keyInfo);

  boolean containsValueForKey(KeyInfo keyInfo);

  Object findObject(KeyInfo keyInfo, boolean isCreate,
      boolean generateCallbacks, Object value, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent);

  Object getEntryForIterator(KeyInfo keyInfo, boolean allowTombstone);

  boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed);

  int entryCount();

  Set getRegionKeysForIteration(LocalRegion currRegion);

  void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      LocalRegion region);
  void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      LocalRegion region);

  void cleanup();

}
