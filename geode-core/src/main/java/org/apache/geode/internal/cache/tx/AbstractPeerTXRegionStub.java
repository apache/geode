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

import java.util.Set;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXStateStub;

public abstract class AbstractPeerTXRegionStub implements TXRegionStub {

  protected final TXStateStub state;

  public AbstractPeerTXRegionStub(TXStateStub txstate) {
    this.state = txstate;
  }

  protected abstract InternalRegion getRegion();

  @Override
  public Set getRegionKeysForIteration() {
    try {
      RemoteFetchKeysMessage.FetchKeysResponse response =
          RemoteFetchKeysMessage.send((LocalRegion) getRegion(), state.getTarget());
      return response.waitForKeys();
    } catch (RegionDestroyedException regionDestroyedException) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              regionDestroyedException.getRegionFullPath()),
          regionDestroyedException);
    } catch (CacheClosedException cacheClosedException) {
      throw new TransactionDataNodeHasDepartedException("Cache was closed while fetching keys");
    } catch (TransactionException transactionException) {
      throw transactionException;
    } catch (Exception exception) {
      throw new TransactionException(exception);
    }
  }

  @Override
  public int entryCount() {
    try {
      RemoteSizeMessage.SizeResponse response =
          RemoteSizeMessage.send(this.state.getTarget(), getRegion());
      return response.waitForSize();
    } catch (RegionDestroyedException regionDestroyedException) {
      throw new TransactionDataNotColocatedException(
          String.format("Region %s not colocated with other regions in transaction",
              regionDestroyedException.getRegionFullPath()),
          regionDestroyedException);
    } catch (CacheClosedException cacheClosedException) {
      throw new TransactionDataNodeHasDepartedException(
          "Cache was closed while performing size operation");
    } catch (TransactionException transactionException) {
      throw transactionException;
    } catch (Exception exception) {
      throw new TransactionException(exception);
    }
  }
}
