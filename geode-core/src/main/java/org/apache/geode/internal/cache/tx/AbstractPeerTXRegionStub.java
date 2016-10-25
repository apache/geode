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

import java.util.Set;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXStateStub;
import org.apache.geode.internal.cache.partitioned.RemoteFetchKeysMessage;
import org.apache.geode.internal.i18n.LocalizedStrings;

public abstract class AbstractPeerTXRegionStub implements TXRegionStub {
  
  protected final TXStateStub state;
  protected final LocalRegion region;

  public AbstractPeerTXRegionStub(TXStateStub txstate, LocalRegion r) {
    this.state = txstate;
    this.region = r;
  }

  @Override
  public Set getRegionKeysForIteration(LocalRegion currRegion) {
    try {
      RemoteFetchKeysMessage.FetchKeysResponse response = RemoteFetchKeysMessage.send(currRegion, state.getTarget());
      return response.waitForKeys();
    } catch (RegionDestroyedException e) {
      throw new TransactionDataNotColocatedException(LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
              .toLocalizedString(e.getRegionFullPath()), e);
    } catch(CacheClosedException e) {
      throw new TransactionDataNodeHasDepartedException("Cache was closed while fetching keys");
    } catch (Exception e) {
      throw new TransactionException(e);
    }
  }



}
