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
