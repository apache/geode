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

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXStateStub;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.RemoteContainsKeyValueMessage.RemoteContainsKeyValueResponse;
import org.apache.geode.internal.cache.tx.RemoteOperationMessage.RemoteOperationResponse;
import org.apache.geode.internal.cache.tx.RemotePutMessage.PutResult;
import org.apache.geode.internal.cache.tx.RemotePutMessage.RemotePutResponse;

public class DistributedTXRegionStub extends AbstractPeerTXRegionStub {

  private final DistributedRegion region;

  public DistributedTXRegionStub(TXStateStub txstate, DistributedRegion r) {
    super(txstate);
    this.region = r;
  }


  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) {
    try {
      RemoteOperationResponse response = RemoteDestroyMessage.send(state.getTarget(),
          event.getRegion(), event, expectedOldValue, true, false);
      response.waitForRemoteResponse();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }


  public Entry getEntry(KeyInfo keyInfo, boolean allowTombstone) {
    try {
      // TODO change RemoteFetchEntryMessage to allow tombstones to be returned
      RemoteFetchEntryMessage.FetchEntryResponse res = RemoteFetchEntryMessage
          .send((InternalDistributedMember) state.getTarget(), region, keyInfo.getKey());
      return res.waitForResponse();
    } catch (EntryNotFoundException enfe) {
      return null;
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (TransactionException e) {
      throw e;
    } catch (CacheException | RemoteOperationException e) {
      throw new TransactionDataNodeHasDepartedException(e);
    }
  }

  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    try {
      RemoteOperationResponse response =
          RemoteInvalidateMessage.send(state.getTarget(), event.getRegion(), event, true, false);
      response.waitForRemoteResponse();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }


  public boolean containsKey(KeyInfo keyInfo) {
    try {
      RemoteContainsKeyValueResponse response = RemoteContainsKeyValueMessage
          .send((InternalDistributedMember) state.getTarget(), region, keyInfo.getKey(), false);
      return response.waitForContainsResult();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }


  public boolean containsValueForKey(KeyInfo keyInfo) {
    try {
      RemoteContainsKeyValueResponse response = RemoteContainsKeyValueMessage
          .send((InternalDistributedMember) state.getTarget(), region, keyInfo.getKey(), true);
      return response.waitForContainsResult();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }


  public Object findObject(KeyInfo keyInfo, boolean isCreate, boolean generateCallbacks,
      Object value, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent) {
    Object retVal = null;
    final Object key = keyInfo.getKey();
    final Object callbackArgument = keyInfo.getCallbackArg();
    try {
      RemoteGetMessage.RemoteGetResponse response =
          RemoteGetMessage.send((InternalDistributedMember) state.getTarget(), region, key,
              callbackArgument, requestingClient);
      retVal = response.waitForResponse(preferCD);
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
    return retVal;
  }


  public Object getEntryForIterator(KeyInfo keyInfo, boolean allowTombstone) {
    return getEntry(keyInfo, allowTombstone);
  }


  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    boolean retVal = false;
    final InternalRegion r = event.getRegion();

    try {
      RemotePutResponse response = RemotePutMessage.txSend(state.getTarget(), r, event,
          lastModified, ifNew, ifOld, expectedOldValue, requireOldValue);
      PutResult result = response.waitForResult();
      event.setOldValue(result.oldValue, true/* force */);
      retVal = result.returnValue;
    } catch (TransactionDataNotColocatedException enfe) {
      throw enfe;
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (CacheException | RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
    return retVal;
  }

  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion region) {
    try {
      RemotePutAllMessage.PutAllResponse response =
          RemotePutAllMessage.send(state.getTarget(), putallOp.getBaseEvent(),
              putallOp.getPutAllEntryData(), putallOp.getPutAllEntryData().length, true, false);
      response.waitForRemoteResponse();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion region) {
    try {
      RemoteRemoveAllMessage.RemoveAllResponse response =
          RemoteRemoveAllMessage.send(state.getTarget(), op.getBaseEvent(),
              op.getRemoveAllEntryData(), op.getRemoveAllEntryData().length, true, false);
      response.waitForRemoteResponse();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              rde.getRegionFullPath()),
          rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }

  @Override
  public void cleanup() {}

  @Override
  protected InternalRegion getRegion() {
    return this.region;
  }
}
