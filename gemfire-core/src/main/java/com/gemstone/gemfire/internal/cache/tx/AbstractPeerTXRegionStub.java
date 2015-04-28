/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tx;

import java.util.Set;

import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionDataNotColocatedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TXStateStub;
import com.gemstone.gemfire.internal.cache.partitioned.RemoteFetchKeysMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public abstract class AbstractPeerTXRegionStub implements TXRegionStub {
  
  protected final TXStateStub state;
  protected final LocalRegion region;

  public AbstractPeerTXRegionStub(TXStateStub txstate, LocalRegion r) {
    this.state = txstate;
    this.region = r;
  }

  public Set getRegionKeysForIteration(LocalRegion currRegion) {
    /*
     * txtodo: not sure about c/s for this?
     */
    try {
      RemoteFetchKeysMessage.FetchKeysResponse response = RemoteFetchKeysMessage.send(currRegion, state.getTarget());
      return response.waitForKeys();
    } catch (RegionDestroyedException e) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
              .toLocalizedString(e.getRegionFullPath()), e);
    } catch (Exception e) {
      throw new TransactionException(e);
    }
  }



}
