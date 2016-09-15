/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.tx;

import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;

import java.util.Set;

public class ClientTXRegionStub implements TXRegionStub {

//  private final LocalRegion region;
  private final ServerRegionDataAccess proxy;
  public ClientTXRegionStub(LocalRegion region) {
//    this.region = region;
    this.proxy = region.getServerProxy(); 
  }
  
  
  public boolean containsKey(KeyInfo keyInfo) {
    return proxy.containsKey(keyInfo.getKey());
  }

  
  public boolean containsValueForKey(KeyInfo keyInfo) {
    return proxy.containsValueForKey(keyInfo.getKey());
  }

  
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) {
	if(event.getOperation().isLocal()) {
	  throw new UnsupportedOperationInTransactionException();
	}
    Object result = proxy.destroy(event.getKey(), expectedOldValue, event.getOperation(), event, event.getCallbackArgument());
    if (result instanceof EntryNotFoundException) {
      throw (EntryNotFoundException)result;
    }

  }

  
  public Object findObject(KeyInfo keyInfo, boolean isCreate,
                           boolean generateCallbacks, Object value, boolean preferCD,
                           ClientProxyMembershipID requestingClient, EntryEventImpl event) {
    return proxy.get(keyInfo.getKey(), keyInfo.getCallbackArg(), event);
  }

  
  public Entry<?,?> getEntry(KeyInfo keyInfo, boolean allowTombstones) {
    return proxy.getEntry(keyInfo.getKey());
  }

  
  public Object getEntryForIterator(KeyInfo keyInfo, boolean allowTombstones) {
    return getEntry(keyInfo, allowTombstones);
  }

  
  public void invalidateExistingEntry(EntryEventImpl event,
      boolean invokeCallbacks, boolean forceNewEntry) {
	  if(event.getOperation().isLocal()) {
	    throw new UnsupportedOperationInTransactionException();
	  }
	  proxy.invalidate(event);

  }

  
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    if (event.isBulkOpInProgress()) {
      // this is a put all, ignore this!
      return true;
    }
    Object result = null;
    try {
      result = proxy.put(event.getKey(), event.getRawNewValue(), event.getDeltaBytes(),
        event, event.getOperation(), requireOldValue,
        expectedOldValue, event.getCallbackArgument(), event.isCreate());
    } catch (ServerOperationException e) {
      if (e.getCause() != null && (e.getCause() instanceof EntryExistsException)) {
        throw (EntryExistsException)e.getCause();
      }
      throw e;
    }
    if (event.getOperation() == Operation.REPLACE) {
      if (!requireOldValue) { // replace(K,V,V)
        return ((Boolean)result).booleanValue();
      } else { // replace(K,V)
        event.setOldValue(result);
      }
    } else if (event.getOperation() == Operation.PUT_IF_ABSENT) {
//      if (logger.isDebugEnabled()) {
//        logger.debug("putIfAbsent for " + event.getKey() + " is returning " + result);
//      }
      event.setOldValue(result);
      if (result != null) {
        return false;
      }
    }
    return true;
  }

  
  public int entryCount() {
    return proxy.size();
  }

  
  public Set getRegionKeysForIteration(LocalRegion currRegion) {
    return proxy.keySet();
  }
  
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts, LocalRegion r) {
	  /*
	   * Don't do anything here , it's handled in proxy and elsewhere.
	   */
  }
  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps, LocalRegion region) {
    // Don't do anything here , it's handled in proxy and elsewhere.
  }


  @Override
  public void cleanup() {
  }
  

}
