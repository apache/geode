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

/*
 * EntryExpiryTask represents a timeout event for a region entry.
 */

import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

public class EntryExpiryTask extends ExpiryTask {

  private static final Logger logger = LogService.getLogger();
  /**
   * The region entry we are working with
   */
  private RegionEntry re; // not final so cancel can null it out see bug 37574

  /*
   * This was added to accommodate a session replication requirement where an empty client has a
   * need to access the expired entry so that additional processing can be performed on it.
   *
   * This field is nether private nor final so that dunits can manipulate it as necessary.
   */
  @MutableForTesting
  public static boolean expireSendsEntryAsCallback =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "EXPIRE_SENDS_ENTRY_AS_CALLBACK");

  protected EntryExpiryTask(LocalRegion region, RegionEntry re) {
    super(region);
    this.re = re;
  }

  @Override
  protected ExpirationAttributes getTTLAttributes() {
    return getLocalRegion().getAttributes().getEntryTimeToLive();
  }

  @Override
  protected ExpirationAttributes getIdleAttributes() {
    return getLocalRegion().getAttributes().getEntryIdleTimeout();
  }

  protected RegionEntry getRegionEntry() {
    return this.re;
  }

  /**
   * Returns the tasks region entry if it "checks" out. The check is to see if the region entry
   * still exists.
   *
   * @throws EntryNotFoundException if the task no longer has a region entry or if the region entry
   *         it has is removed.
   */
  protected RegionEntry getCheckedRegionEntry() throws EntryNotFoundException {
    RegionEntry result = this.re;
    if (re == null || re.isDestroyedOrRemoved()) {
      throw new EntryNotFoundException("expiration task no longer has access to region entry");
    }
    return result;
  }

  @Override
  protected long getLastAccessedTime() throws EntryNotFoundException {
    RegionEntry re = getCheckedRegionEntry();
    try {
      return re.getLastAccessed();
    } catch (InternalStatisticsDisabledException e) {
      return 0;
    }
  }

  @Override
  protected long getLastModifiedTime() throws EntryNotFoundException {
    return getCheckedRegionEntry().getLastModified();
  }

  private Object getValueForCallback(LocalRegion r, Object k) {
    Region.Entry<?, ?> e = r.getEntry(k);
    return (e != null) ? e.getValue() : null;
  }

  private Object createExpireEntryCallback(LocalRegion r, Object k) {
    return expireSendsEntryAsCallback ? getValueForCallback(r, k) : null;
  }

  @Override
  protected boolean destroy(boolean isPending) throws CacheException {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    @Released
    EntryEventImpl event = EntryEventImpl.create(lr, Operation.EXPIRE_DESTROY, key, null,
        createExpireEntryCallback(lr, key), false, lr.getMyId());
    try {
      event.setPendingSecondaryExpireDestroy(isPending);
      if (lr.generateEventID()) {
        event.setNewEventId(lr.getCache().getDistributedSystem());
      }
      lr.expireDestroy(event, true); // expectedOldValue
      return true;
    } finally {
      event.release();
    }
  }

  @Override
  protected boolean invalidate() throws TimeoutException, EntryNotFoundException {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    @Released
    EntryEventImpl event = EntryEventImpl.create(lr, Operation.EXPIRE_INVALIDATE, key, null,
        createExpireEntryCallback(lr, key), false, lr.getMyId());
    try {
      if (lr.generateEventID()) {
        event.setNewEventId(lr.getCache().getDistributedSystem());
      }
      lr.expireInvalidate(event);
    } finally {
      event.release();
    }
    return true;
  }

  @Override
  protected boolean localDestroy() throws CacheException {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    @Released
    EntryEventImpl event = EntryEventImpl.create(lr, Operation.EXPIRE_LOCAL_DESTROY, key, null,
        createExpireEntryCallback(lr, key), false, lr.getMyId());
    try {
      if (lr.generateEventID()) {
        event.setNewEventId(lr.getCache().getDistributedSystem());
      }
      lr.expireDestroy(event, false); // expectedOldValue
    } finally {
      event.release();
    }
    return true;
  }

  @Override
  protected boolean localInvalidate() throws EntryNotFoundException {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    @Released
    EntryEventImpl event = EntryEventImpl.create(lr, Operation.EXPIRE_LOCAL_INVALIDATE, key, null,
        createExpireEntryCallback(lr, key), false, lr.getMyId());
    try {
      if (lr.generateEventID()) {
        event.setNewEventId(lr.getCache().getDistributedSystem());
      }
      lr.expireInvalidate(event);
    } finally {
      event.release();
    }
    return true;
  }

  @Override
  protected void reschedule() throws CacheException {
    if (isCacheClosing() || getLocalRegion().isClosed() || getLocalRegion().isDestroyed()
        || !isExpirationAllowed()) {
      return;
    }
    if (getExpirationTime() > 0) {
      addExpiryTask();
      if (expiryTaskListener != null) {
        expiryTaskListener.afterReschedule(this);
      }
    }
  }

  @Override
  protected void addExpiryTask() throws EntryNotFoundException {
    getLocalRegion().addExpiryTask(getCheckedRegionEntry());
  }

  @Override
  public String toString() {
    String result = super.toString();
    RegionEntry re = this.re;
    if (re != null) {
      result += ", " + re.getKey();
    }
    return result;
  }

  @Override
  protected void performTimeout() throws CacheException {
    // remove the task from the region's map first thing
    // so the next call to addExpiryTaskIfAbsent will
    // add a new task instead of doing nothing, which would
    // erroneously cancel expiration for this key.
    getLocalRegion().cancelExpiryTask(this.re, this);
    getLocalRegion().performExpiryTimeout(this);
  }

  @Override
  public boolean isPending() {
    RegionEntry re = this.re;
    if (re == null) {
      return false;
    }
    if (re.isDestroyedOrRemoved()) {
      return false;
    }
    ExpirationAction action = getAction();
    if (action == null) {
      return false;
    }
    if ((action.isInvalidate() || action.isLocalInvalidate()) && re.isInvalid()) {
      return false;
    }
    return true;
  }

  @Override
  protected ExpirationAction getAction() {
    long ttl = getTTLAttributes().getTimeout();
    long idle = getIdleAttributes().getTimeout();
    ExpirationAction action;
    if (ttl == 0) {
      action = getIdleAttributes().getAction();
    } else if (idle != 0 && idle < ttl) {
      action = getIdleAttributes().getAction();
    } else {
      action = getTTLAttributes().getAction();
    }
    return action;
  }

  @Override
  protected boolean isIdleExpiredOnOthers() throws EntryNotFoundException {
    if (getIdleAttributes().getTimeout() <= 0L) {
      // idle expiration is not being used
      return true;
    }
    if (getIdleAttributes().getAction().isLocal()) {
      // no need to consult with others if using a local action
      return true;
    }
    if (SystemPropertyHelper.restoreIdleExpirationBehavior()) {
      return true;
    }

    long latestLastAccessTime = getLatestLastAccessTimeOnOtherMembers();
    if (latestLastAccessTime > getLastAccessedTime()) {
      setLastAccessedTime(latestLastAccessTime);
      return false;
    }
    return true;
  }

  private long getLatestLastAccessTimeOnOtherMembers() {
    return getLocalRegion().getLatestLastAccessTimeFromOthers(getKey());
  }

  private void setLastAccessedTime(long lastAccessedTime) throws EntryNotFoundException {
    RegionEntry re = getCheckedRegionEntry();
    re.setLastAccessed(lastAccessedTime);
  }

  /**
   * Called by LocalRegion#performExpiryTimeout
   */
  @Override
  protected void basicPerformTimeout(boolean isPending) throws CacheException {
    if (!isExpirationAllowed()) {
      return;
    }
    if (!isExpirationPossible()) {
      reschedule();
      return;
    }
    // Need to figure out why it expired - ttl, or idle timeout?
    ExpirationAction action;
    long ttl = getTTLAttributes().getTimeout();
    long idle = getIdleAttributes().getTimeout();
    if (ttl == 0) {
      action = getIdleAttributes().getAction();
    } else if (idle != 0 && idle < ttl) {
      action = getIdleAttributes().getAction();
    } else {
      action = getTTLAttributes().getAction();
    }
    // if global scope get distributed lock for destroy and invalidate actions
    if (getLocalRegion().getScope().isGlobal() && (action.isDestroy() || action.isInvalidate())) {
      Lock lock = getLocalRegion().getDistributedLock(getCheckedRegionEntry().getKey());
      lock.lock();
      try {
        long expTime = getExpirationTime();
        if (expTime == 0L) {
          return;
        }
        if (hasExpired(getNow(), expTime)) {
          if (logger.isTraceEnabled()) {
            // NOTE: original finer message used this.toString() twice
            logger.trace(
                "{}.performTimeout().getExpirationTime() is {}; {}.expire({}). ttlExpiration: {}, idleExpiration: {}, ttlAttrs: {}, idleAttrs: {} action is: {}",
                this, expTime, this, action, ttl, idle, getTTLAttributes(), getIdleAttributes());
          }
          expire(action, isPending);
          return;
        }
      } finally {
        lock.unlock();
      }
    } else {
      if (logger.isTraceEnabled()) {
        logger.trace("{}..performTimeout().getExpirationTime() is {}", this, getExpirationTime());
      }
      expire(isPending);
      return;
    }
    reschedule();
  }

  @Override
  public Object getKey() {
    RegionEntry entry = this.re;
    if (entry == null) {
      throw new EntryDestroyedException();
    }
    return entry.getKey();
  }

  @Override
  public boolean cancel() {
    boolean superCancel = super.cancel();
    if (superCancel) {
      this.re = null;
      if (expiryTaskListener != null) {
        expiryTaskListener.afterCancel(this);
      }
    }
    return superCancel;
  }

}
