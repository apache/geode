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
package com.gemstone.gemfire.internal.cache;

import java.util.Hashtable;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.StatisticsDisabledException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/** ******************* Class Entry ***************************************** */

public class TXEntry implements Region.Entry
{
  /**
   * 
   */
  private final LocalRegion localRegion;

  protected final KeyInfo keyInfo;

  private boolean entryIsDestroyed = false;
  private final boolean rememberReads;

  private final TXStateInterface myTX;

  /**
   * Create an Entry given a key. The returned Entry may or may not be
   * destroyed
   */
  TXEntry(LocalRegion localRegion, KeyInfo key, TXStateInterface tx) {
    this(localRegion, key, tx, true/*rememberReads*/);
  }
  TXEntry(LocalRegion localRegion, KeyInfo key, TXStateInterface tx, boolean rememberReads) {
    this.localRegion = localRegion;
    this.localRegion.validateKey(key.getKey());
    this.keyInfo = key;
    this.myTX = tx;
    this.rememberReads = rememberReads;
    
    //Assert that these contructors are invoked only 
    // via factory path. I am not able to make them private
    // because SqlfabricTxEntry needs extending TxEntry
    /*if(logger.isDebugEnabled()) {
      StackTraceElement[] traces =Thread.currentThread().getStackTrace();
      //The third element should be the factory one
      StackTraceElement trace = traces[2];
      Assert.assertTrue(TxEntryFactory.class.isAssignableFrom(trace.getClass()));
      Assert.assertTrue(trace.getMethodName().equals("createEntry"));
    }*/
  }

  public boolean isLocal() {
    return true;
  }

  protected void checkTX()
  {
    // Protect against the case where this instance was handed to a different thread w/ or w/o a transaction
    // Protect against the case where the transaction associated with this entry is not in progress 
    if (! this.myTX.isInProgressAndSameAs(this.localRegion.getTXState())) {
      throw new IllegalStateException(LocalizedStrings.LocalRegion_REGIONENTRY_WAS_CREATED_WITH_TRANSACTION_THAT_IS_NO_LONGER_ACTIVE.toLocalizedString(this.myTX.getTransactionId()));
    } 
  }

  public boolean isDestroyed()
  {
    if (this.entryIsDestroyed) {
      return true;
    }
    checkTX();
    try {
      if (!this.myTX.containsKey(this.keyInfo, this.localRegion)) {
        this.entryIsDestroyed = true;
      }
    }
    catch (RegionDestroyedException ex) {
      this.entryIsDestroyed = true;
    }
    catch (CancelException ex) {
      this.entryIsDestroyed = true;
    }
    return this.entryIsDestroyed;
  }

  public Object getKey()
  {
    checkEntryDestroyed();
    return this.keyInfo.getKey();
  }

  @Unretained
  public Object getValue()
  {
    checkTX();
//    Object value = this.localRegion.getDeserialized(this.key, false, this.myTX, this.rememberReads);
    @Unretained Object value = this.myTX.getDeserializedValue(keyInfo, this.localRegion, false, false, false, null, false,
      false);
    if (value == null) {
      throw new EntryDestroyedException(this.keyInfo.getKey().toString());
    }
    else if (Token.isInvalid(value)) {
      return null;
    }

    return value;
  }

  public Region getRegion()
  {
    checkEntryDestroyed();
    return this.localRegion;
  }

  public CacheStatistics getStatistics()
  {
    // prefer entry destroyed exception over statistics disabled exception
    checkEntryDestroyed();
    checkTX();
    if (!this.localRegion.statisticsEnabled) {
      throw new StatisticsDisabledException(LocalizedStrings.LocalRegion_STATISTICS_DISABLED_FOR_REGION_0.toLocalizedString(this.localRegion.getFullPath()));
    }
    // On a TXEntry stats are non-existent so return a dummy impl
    return new CacheStatistics() {
      public long getLastModifiedTime()
      {
        return (getRegion() != null) ? ((LocalRegion) getRegion())
            .cacheTimeMillis() : System.currentTimeMillis();
      }

      public long getLastAccessedTime()
      {
        return (getRegion() != null) ? ((LocalRegion) getRegion())
            .cacheTimeMillis() : System.currentTimeMillis();
      }

      public long getMissCount()
      {
        return 0;
      }

      public long getHitCount()
      {
        return 0;
      }

      public float getHitRatio()
      {
        return 0;
      }

      public void resetCounts()
      {
      }
    };
  }

  public Object getUserAttribute()
  {
    checkTX();
    throwIfUAOperationForPR();
    TXEntryUserAttrState tx = txReadUA(this.keyInfo);
    if (tx != null) {
      return tx.getPendingValue();
    }
    else {
      checkEntryDestroyed();
      return this.localRegion.basicGetEntryUserAttribute(this.keyInfo.getKey());
    }
  }

  public Object setUserAttribute(Object value)
  {
    checkTX();
    throwIfUAOperationForPR();
    TXEntryUserAttrState tx = txWriteUA(this.keyInfo);
    if (tx != null) {
      return tx.setPendingValue(value);
    }
    else {
      checkEntryDestroyed();
      if (this.localRegion.entryUserAttributes == null) {
        this.localRegion.entryUserAttributes = new Hashtable();
      }
      return this.localRegion.entryUserAttributes.put(keyInfo, value);
    }
  }

  private void throwIfUAOperationForPR() {
    if (this.localRegion instanceof PartitionedRegion) {
      throw new UnsupportedOperationException(LocalizedStrings.
          TXEntry_UA_NOT_SUPPORTED_FOR_PR.toLocalizedString());
    }
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof TXEntry)) {
      return false;
    }
    TXEntry lre = (TXEntry)obj;
    return this.keyInfo.getKey().equals(lre.keyInfo.getKey()) && this.getRegion() == lre.getRegion();
  }

  @Override
  public int hashCode()
  {
    return this.keyInfo.getKey().hashCode() ^ this.getRegion().hashCode();
  }

  ////////////////// Private Methods
  // /////////////////////////////////////////

  /*
   * throws CacheClosedException or EntryDestroyedException if this entry is
   * destroyed.
   */
  private void checkEntryDestroyed()
  {
    if (isDestroyed()) {
      throw new EntryDestroyedException(this.keyInfo.getKey().toString());
    }
  }

  private final TXEntryUserAttrState txReadUA(KeyInfo ki)
  {
    TXRegionState txr = this.myTX.txReadRegion(this.localRegion);
    if (txr != null) {
      return txr.readEntryUserAttr(ki.getKey());
    }
    else {
      return null;
    }
  }

  protected final TXEntryUserAttrState txWriteUA(KeyInfo ki)
  {
    TXRegionState txr = myTX.txWriteRegion(this.localRegion, ki);
    if (txr != null) {
      return txr.writeEntryUserAttr(ki.getKey(), this.localRegion);
    }
    else {
      return null;
    }
  }

  /**
   * @since GemFire 5.0
   */
  public Object setValue(Object arg0)
  {
    return this.localRegion.put(this.getKey(), arg0);
  }
  
}
