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
package org.apache.geode.internal.cache;


import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.internal.offheap.annotations.Unretained;

/** ******************* Class Entry ***************************************** */

public class TXEntry implements Region.Entry {
  private final LocalRegion localRegion;

  protected final KeyInfo keyInfo;

  private boolean entryIsDestroyed = false;
  private final boolean rememberReads;

  private final TXStateInterface myTX;

  /**
   * Create an Entry given a key. The returned Entry may or may not be destroyed
   */
  TXEntry(LocalRegion localRegion, KeyInfo key, TXStateInterface tx) {
    this(localRegion, key, tx, true/* rememberReads */);
  }

  TXEntry(LocalRegion localRegion, KeyInfo key, TXStateInterface tx, boolean rememberReads) {
    this.localRegion = localRegion;
    this.localRegion.validateKey(key.getKey());
    keyInfo = key;
    myTX = tx;
    this.rememberReads = rememberReads;
  }

  @Override
  public boolean isLocal() {
    return true;
  }

  protected void checkTX() {
    // Protect against the case where this instance was handed to a different thread w/ or w/o a
    // transaction
    // Protect against the case where the transaction associated with this entry is not in progress
    if (!myTX.isInProgressAndSameAs(localRegion.getTXState())) {
      throw new IllegalStateException(
          "Region.Entry was created with transaction that is no longer active.");
    }
  }

  @Override
  public boolean isDestroyed() {
    if (entryIsDestroyed) {
      return true;
    }
    checkTX();
    try {
      if (!myTX.containsKey(keyInfo, localRegion)) {
        entryIsDestroyed = true;
      }
    } catch (RegionDestroyedException ex) {
      entryIsDestroyed = true;
    } catch (CancelException ex) {
      entryIsDestroyed = true;
    }
    return entryIsDestroyed;
  }

  @Override
  public Object getKey() {
    checkEntryDestroyed();
    return keyInfo.getKey();
  }

  @Override
  @Unretained
  public Object getValue() {
    return getValue(true);
  }

  @Unretained
  public Object getValue(boolean createIfAbsent) {
    checkTX();
    // Object value = this.localRegion.getDeserialized(this.key, false, this.myTX,
    // this.rememberReads);
    @Unretained
    Object value = myTX.getDeserializedValue(keyInfo, localRegion, false, false, false,
        null, false, false, createIfAbsent);
    if (value == null) {
      throw new EntryDestroyedException(keyInfo.getKey().toString());
    } else if (Token.isInvalid(value)) {
      return null;
    }

    return value;
  }

  @Override
  public Region getRegion() {
    checkEntryDestroyed();
    return localRegion;
  }

  @Override
  public CacheStatistics getStatistics() {
    // prefer entry destroyed exception over statistics disabled exception
    checkEntryDestroyed();
    checkTX();
    if (!localRegion.statisticsEnabled) {
      throw new StatisticsDisabledException(
          String.format("Statistics disabled for region '%s'",
              localRegion.getFullPath()));
    }
    // On a TXEntry stats are non-existent so return a dummy impl
    return new CacheStatistics() {
      @Override
      public long getLastModifiedTime() {
        return (getRegion() != null) ? ((LocalRegion) getRegion()).cacheTimeMillis()
            : System.currentTimeMillis();
      }

      @Override
      public long getLastAccessedTime() {
        return (getRegion() != null) ? ((LocalRegion) getRegion()).cacheTimeMillis()
            : System.currentTimeMillis();
      }

      @Override
      public long getMissCount() {
        return 0;
      }

      @Override
      public long getHitCount() {
        return 0;
      }

      @Override
      public float getHitRatio() {
        return 0;
      }

      @Override
      public void resetCounts() {}
    };
  }

  @Override
  public Object getUserAttribute() {
    checkTX();
    throwIfUAOperationForPR();
    TXEntryUserAttrState tx = txReadUA(keyInfo);
    if (tx != null) {
      return tx.getPendingValue();
    } else {
      checkEntryDestroyed();
      return localRegion.basicGetEntryUserAttribute(keyInfo.getKey());
    }
  }

  @Override
  public Object setUserAttribute(Object value) {
    checkTX();
    throwIfUAOperationForPR();
    TXEntryUserAttrState tx = txWriteUA(keyInfo);
    if (tx != null) {
      return tx.setPendingValue(value);
    } else {
      checkEntryDestroyed();
      return localRegion.getEntryUserAttributes().put(keyInfo, value);
    }
  }

  private void throwIfUAOperationForPR() {
    if (localRegion instanceof PartitionedRegion) {
      throw new UnsupportedOperationException(
          "Partitioned region does not support UserAttributes in transactional context");
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TXEntry)) {
      return false;
    }
    TXEntry lre = (TXEntry) obj;
    return keyInfo.getKey().equals(lre.keyInfo.getKey())
        && getRegion() == lre.getRegion();
  }

  @Override
  public int hashCode() {
    return keyInfo.getKey().hashCode() ^ getRegion().hashCode();
  }

  ////////////////// Private Methods
  // /////////////////////////////////////////

  /*
   * throws CacheClosedException or EntryDestroyedException if this entry is destroyed.
   */
  private void checkEntryDestroyed() {
    if (isDestroyed()) {
      throw new EntryDestroyedException(keyInfo.getKey().toString());
    }
  }

  private TXEntryUserAttrState txReadUA(KeyInfo ki) {
    TXRegionState txr = myTX.txReadRegion(localRegion);
    if (txr != null) {
      return txr.readEntryUserAttr(ki.getKey());
    } else {
      return null;
    }
  }

  protected TXEntryUserAttrState txWriteUA(KeyInfo ki) {
    TXRegionState txr = myTX.txWriteRegion(localRegion, ki);
    if (txr != null) {
      return txr.writeEntryUserAttr(ki.getKey(), localRegion);
    } else {
      return null;
    }
  }

  /**
   * @since GemFire 5.0
   */
  @Override
  public Object setValue(Object arg0) {
    return localRegion.put(getKey(), arg0);
  }

}
