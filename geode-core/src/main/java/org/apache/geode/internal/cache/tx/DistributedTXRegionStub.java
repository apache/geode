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

import java.util.Collections;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegionException;
import org.apache.geode.internal.cache.RemoteContainsKeyValueMessage;
import org.apache.geode.internal.cache.RemoteDestroyMessage;
import org.apache.geode.internal.cache.RemoteFetchEntryMessage;
import org.apache.geode.internal.cache.RemoteGetMessage;
import org.apache.geode.internal.cache.RemoteInvalidateMessage;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.RemotePutAllMessage;
import org.apache.geode.internal.cache.RemotePutMessage;
import org.apache.geode.internal.cache.RemoteRemoveAllMessage;
import org.apache.geode.internal.cache.TXStateStub;
import org.apache.geode.internal.cache.RemoteContainsKeyValueMessage.RemoteContainsKeyValueResponse;
import org.apache.geode.internal.cache.RemoteOperationMessage.RemoteOperationResponse;
import org.apache.geode.internal.cache.RemotePutMessage.PutResult;
import org.apache.geode.internal.cache.RemotePutMessage.RemotePutResponse;
import org.apache.geode.internal.cache.partitioned.RemoteSizeMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.i18n.LocalizedStrings;

public class DistributedTXRegionStub extends AbstractPeerTXRegionStub {
  
  private final LocalRegion region;
  
  public DistributedTXRegionStub(TXStateStub txstate,LocalRegion r) {
   super(txstate,r);
   this.region = r;
  }

  
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) {
    // TODO Auto-generated method stub
        //this.prStats.incPartitionMessagesSent();
        try {
          RemoteOperationResponse response = RemoteDestroyMessage.send(state.getTarget(),
            event.getLocalRegion(),
            event,
            expectedOldValue, DistributionManager.PARTITIONED_REGION_EXECUTOR, true, false);
          response.waitForCacheException();
        }
        catch (EntryNotFoundException enfe) {
          throw enfe;
        }catch (TransactionDataNotColocatedException enfe) {
          throw enfe;
        }
        catch (CacheException ce) {
          throw new PartitionedRegionException(LocalizedStrings.PartitionedRegion_DESTROY_OF_ENTRY_ON_0_FAILED.toLocalizedString(state.getTarget()), ce);
        }
        catch (RegionDestroyedException rde) {
          throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
              .toLocalizedString(rde.getRegionFullPath()), rde);
        } catch(RemoteOperationException roe) {
          throw new TransactionDataNodeHasDepartedException(roe);
        }
  }

  
  public Entry getEntry(KeyInfo keyInfo, boolean allowTombstone) {
      try {
        // TODO change RemoteFetchEntryMessage to allow tombstones to be returned
        RemoteFetchEntryMessage.FetchEntryResponse res = RemoteFetchEntryMessage.send((InternalDistributedMember)state.getTarget(), region, keyInfo.getKey());
        //this.prStats.incPartitionMessagesSent();
        return res.waitForResponse();
      } catch (EntryNotFoundException enfe) {
        return null;
      } catch (RegionDestroyedException rde) {
        throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
                .toLocalizedString(rde.getRegionFullPath()), rde);
      } catch (TransactionException e) {
        RuntimeException re = new TransactionDataNotColocatedException(LocalizedStrings.PartitionedRegion_KEY_0_NOT_COLOCATED_WITH_TRANSACTION.toLocalizedString(keyInfo.getKey()));
        re.initCause(e);
        throw re;
      } catch (RemoteOperationException e) {
        throw new TransactionDataNodeHasDepartedException(e);
      }
  }

  
  public void invalidateExistingEntry(EntryEventImpl event,
      boolean invokeCallbacks, boolean forceNewEntry) {
      try {
        RemoteOperationResponse response = RemoteInvalidateMessage.send(state.getTarget(),
            event.getRegion(), event,
            DistributionManager.PARTITIONED_REGION_EXECUTOR, true, false); 
        response.waitForCacheException();
      } catch (RegionDestroyedException rde) {
        throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
            .toLocalizedString(rde.getRegionFullPath()), rde);
      } catch(RemoteOperationException roe) {
        throw new TransactionDataNodeHasDepartedException(roe);
      }
  }

  
  public boolean containsKey(KeyInfo keyInfo) {
    try {
      RemoteContainsKeyValueResponse response = RemoteContainsKeyValueMessage.send((InternalDistributedMember)state.getTarget(),
          region, keyInfo.getKey(), false); 
      return response.waitForContainsResult();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
              .toLocalizedString(rde.getRegionFullPath()), rde);
    } catch(RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }

  
  public boolean containsValueForKey(KeyInfo keyInfo) {
    try {
      RemoteContainsKeyValueResponse response = RemoteContainsKeyValueMessage.send((InternalDistributedMember)state.getTarget(),
          region, keyInfo.getKey(), true); 
      return response.waitForContainsResult();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
          .toLocalizedString(rde.getRegionFullPath()), rde);
    } catch(RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }
  }

  
  public Object findObject(KeyInfo keyInfo,
                           boolean isCreate,
                           boolean generateCallbacks,
                           Object value,
                           boolean preferCD,
                           ClientProxyMembershipID requestingClient,
                           EntryEventImpl clientEvent) {
    Object retVal = null;
    final Object key = keyInfo.getKey();
    final Object callbackArgument = keyInfo.getCallbackArg();
    try {
      RemoteGetMessage.RemoteGetResponse response = RemoteGetMessage.send((InternalDistributedMember)state.getTarget(), region, key,
          callbackArgument, requestingClient);
      retVal = response.waitForResponse(preferCD);
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
          .toLocalizedString(rde.getRegionFullPath()), rde);
    } catch(RemoteOperationException roe) {
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
    final LocalRegion r = event.getLocalRegion();
      
      try {
        RemotePutResponse response = RemotePutMessage.txSend(state.getTarget(),r,event,lastModified,ifNew,ifOld,expectedOldValue,requireOldValue);
        PutResult result = response.waitForResult();
        event.setOldValue(result.oldValue, true/*force*/);
        retVal = result.returnValue;
      }catch (TransactionDataNotColocatedException enfe) {
        throw enfe;
      }
      catch (CacheException ce) {
        throw new PartitionedRegionException(LocalizedStrings.PartitionedRegion_DESTROY_OF_ENTRY_ON_0_FAILED.toLocalizedString(state.getTarget()), ce);
      }
      catch (RegionDestroyedException rde) {
        throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
            .toLocalizedString(rde.getRegionFullPath()), rde);
      } catch(RemoteOperationException roe) {
        throw new TransactionDataNodeHasDepartedException(roe);
      }
    return retVal;
  }

  
  public int entryCount() {
    try {
      RemoteSizeMessage.SizeResponse response = RemoteSizeMessage.send(Collections.singleton(state.getTarget()), region);
      return response.waitForSize();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
          .toLocalizedString(rde.getRegionFullPath()), rde);
    } catch (Exception e) {
      throw new TransactionException(e);
    }
  }
  

  public void postPutAll(DistributedPutAllOperation putallOp,
      VersionedObjectList successfulPuts, LocalRegion region) {
    try {
      RemotePutAllMessage.PutAllResponse response = RemotePutAllMessage.send(state.getTarget(), putallOp.getBaseEvent(), putallOp.getPutAllEntryData(), putallOp.getPutAllEntryData().length, true, DistributionManager.PARTITIONED_REGION_EXECUTOR, false);
      response.waitForCacheException();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
          .toLocalizedString(rde.getRegionFullPath()), rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }  
  }
  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps, LocalRegion region) {
    try {
      RemoteRemoveAllMessage.RemoveAllResponse response = RemoteRemoveAllMessage.send(state.getTarget(), op.getBaseEvent(), op.getRemoveAllEntryData(), op.getRemoveAllEntryData().length, true, DistributionManager.PARTITIONED_REGION_EXECUTOR, false);
      response.waitForCacheException();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
          .toLocalizedString(rde.getRegionFullPath()), rde);
    } catch (RemoteOperationException roe) {
      throw new TransactionDataNodeHasDepartedException(roe);
    }  
  }

  @Override
  public void cleanup() {
  }
}
