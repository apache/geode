/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.tx;

import org.apache.geode.cache.Region.Entry;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;

import java.util.Set;

public interface TXRegionStub {

  void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue);

  Entry getEntry(KeyInfo keyInfo, boolean allowTombstone);

  void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry);

  boolean containsKey(KeyInfo keyInfo);

  boolean containsValueForKey(KeyInfo keyInfo);

  Object findObject(KeyInfo keyInfo, boolean isCreate, boolean generateCallbacks, Object value,
      boolean preferCD, ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent);

  Object getEntryForIterator(KeyInfo keyInfo, boolean allowTombstone);

  boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue, long lastModified, boolean overwriteDestroyed);

  int entryCount();

  Set getRegionKeysForIteration(LocalRegion currRegion);

  void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      LocalRegion region);

  void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      LocalRegion region);

  void cleanup();

}
