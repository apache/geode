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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
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


  @Override
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) {
    try {
      RemoteOperationResponse response = sendRemoteDestroyMessage(event, expectedOldValue);
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

  RemoteOperationResponse sendRemoteDestroyMessage(EntryEventImpl event, Object expectedOldValue)
      throws RemoteOperationException {
    return RemoteDestroyMessage.send(state.getTarget(), event.getRegion(), event, expectedOldValue,
        true, false);
  }


  @Override
  public Entry getEntry(KeyInfo keyInfo, boolean allowTombstone) {
    try {
      // TODO change RemoteFetchEntryMessage to allow tombstones to be returned
      RemoteFetchEntryMessage.FetchEntryResponse res = sendRemoteFetchEntryMessage(
          (InternalDistributedMember) state.getTarget(), region, keyInfo.getKey());
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

  RemoteFetchEntryMessage.FetchEntryResponse sendRemoteFetchEntryMessage(
      InternalDistributedMember recipient, LocalRegion region, Object key)
      throws RemoteOperationException {
    return RemoteFetchEntryMessage.send(recipient, region, key);
  }

  @Override
  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    try {
      RemoteOperationResponse response = sendRemoteInvalidateMessage(state.getTarget(), event);
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

  RemoteOperationResponse sendRemoteInvalidateMessage(DistributedMember recipient,
      EntryEventImpl event) throws RemoteOperationException {
    return RemoteInvalidateMessage.send(recipient, event.getRegion(), event, true, false);
  }


  @Override
  public boolean containsKey(KeyInfo keyInfo) {
    try {
      RemoteContainsKeyValueResponse response = sendRemoteContainsKeyValueMessage(
          (InternalDistributedMember) state.getTarget(), keyInfo.getKey(), false);
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

  @Override
  public boolean containsValueForKey(KeyInfo keyInfo) {
    try {
      RemoteContainsKeyValueResponse response = sendRemoteContainsKeyValueMessage(
          (InternalDistributedMember) state.getTarget(), keyInfo.getKey(), true);
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

  RemoteContainsKeyValueResponse sendRemoteContainsKeyValueMessage(
      InternalDistributedMember recipient, Object key, boolean valueCheck)
      throws RemoteOperationException {
    return RemoteContainsKeyValueMessage.send(recipient, region, key, valueCheck);
  }

  @Override
  public Object findObject(KeyInfo keyInfo, boolean isCreate, boolean generateCallbacks,
      Object value, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent) {
    Object retVal;
    final Object key = keyInfo.getKey();
    final Object callbackArgument = keyInfo.getCallbackArg();
    try {
      RemoteGetMessage.RemoteGetResponse response = sendRemoteGetMessage(
          (InternalDistributedMember) state.getTarget(), key, callbackArgument, requestingClient);
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

  RemoteGetMessage.RemoteGetResponse sendRemoteGetMessage(InternalDistributedMember recipient,
      final Object key, final Object callbackArgument, ClientProxyMembershipID requestingClient)
      throws RemoteOperationException {
    return RemoteGetMessage.send(recipient, region, key, callbackArgument, requestingClient);
  }


  @Override
  public Object getEntryForIterator(KeyInfo keyInfo, boolean allowTombstone) {
    return getEntry(keyInfo, allowTombstone);
  }


  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    boolean retVal;
    final InternalRegion r = event.getRegion();

    try {
      RemotePutResponse response = txSendRemotePutMessage(state.getTarget(), r, event,
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

  RemotePutResponse txSendRemotePutMessage(DistributedMember recipient, InternalRegion r,
      EntryEventImpl event, final long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue) throws RemoteOperationException {
    return RemotePutMessage.txSend(recipient, r, event, lastModified, ifNew, ifOld,
        expectedOldValue, requireOldValue);
  }

  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion region) {
    try {
      RemotePutAllMessage.PutAllResponse response =
          sendRemotePutAllMessage(state.getTarget(), putallOp.getBaseEvent(),
              putallOp.getPutAllEntryData(), putallOp.getPutAllEntryData().length);
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

  RemotePutAllMessage.PutAllResponse sendRemotePutAllMessage(DistributedMember recipient,
      EntryEventImpl event, DistributedPutAllOperation.PutAllEntryData[] putAllData,
      int putAllDataCount) throws RemoteOperationException {
    return RemotePutAllMessage.send(recipient, event,
        putAllData, putAllDataCount, true, false);

  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion region) {
    try {
      RemoteRemoveAllMessage.RemoveAllResponse response =
          sendRemoteRemoveAllMessage(state.getTarget(), op.getBaseEvent(),
              op.getRemoveAllEntryData(), op.getRemoveAllEntryData().length);
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

  RemoteRemoveAllMessage.RemoveAllResponse sendRemoteRemoveAllMessage(DistributedMember recipient,
      EntryEventImpl event, DistributedRemoveAllOperation.RemoveAllEntryData[] removeAllData,
      int removeAllDataCount) throws RemoteOperationException {
    return RemoteRemoveAllMessage.send(recipient, event, removeAllData, removeAllDataCount, true,
        false);

  }

  @Override
  public void cleanup() {}

  @Override
  protected InternalRegion getRegion() {
    return this.region;
  }
}
