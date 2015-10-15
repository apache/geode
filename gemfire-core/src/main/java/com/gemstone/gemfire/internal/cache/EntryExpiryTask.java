/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

/**
 * EntryExpiryTask represents a timeout event for a region entry.
 */

import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.internal.InternalStatisticsDisabledException;
import com.gemstone.gemfire.internal.logging.LogService;

public class EntryExpiryTask extends ExpiryTask {

  private static final Logger logger = LogService.getLogger();
  /**
   * The region entry we are working with
   */
  private RegionEntry re; // not final so cancel can null it out see bug 37574

  /*
   * This was added to accommodate a session replication requirement where an
   * empty client has a need to access the expired entry so that additional
   * processing can be performed on it.
   *
   * This field is nether private nor final so that dunits can manipulate it as
   * necessary.
   */
  public static boolean expireSendsEntryAsCallback =
      Boolean.getBoolean("gemfire.EXPIRE_SENDS_ENTRY_AS_CALLBACK");

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
   * Returns the tasks region entry if it "checks" out. The check is to
   * see if the region entry still exists.
   * @throws EntryNotFoundException if the task no longer has a region entry or
   * if the region entry it has is removed.
   */
  protected RegionEntry getCheckedRegionEntry() throws EntryNotFoundException {
    RegionEntry result = this.re;
    if (re == null || re.isDestroyedOrRemoved()) {
      throw new EntryNotFoundException("expiration task no longer has access to region entry");
    }
    return result;
  }
  
  @Override
  protected long getLastAccessedTime() throws EntryNotFoundException
  {
    RegionEntry re = getCheckedRegionEntry();
    try {
      return re.getLastAccessed();
    }
    catch (InternalStatisticsDisabledException e) {
      return 0;
    }
  }

  @Override
  protected long getLastModifiedTime() throws EntryNotFoundException
  {
    return getCheckedRegionEntry().getLastModified();
  }

  private Object getValueForCallback(LocalRegion r, Object k) {
    Region.Entry<?,?> e = r.getEntry(k);
    return (e != null) ? e.getValue() : null;
  }

  private Object createExpireEntryCallback(LocalRegion r, Object k) {
    return expireSendsEntryAsCallback ? getValueForCallback(r, k) : null;
  }

  @Override
  protected boolean destroy(boolean isPending) throws CacheException
  {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    EntryEventImpl event = EntryEventImpl.create(
        lr, Operation.EXPIRE_DESTROY, key, null,
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
  protected boolean invalidate() throws TimeoutException,
      EntryNotFoundException
  {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    EntryEventImpl event = EntryEventImpl.create(lr,
        Operation.EXPIRE_INVALIDATE, key, null,
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
  protected boolean localDestroy() throws CacheException
  {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    EntryEventImpl event = EntryEventImpl.create(lr,
        Operation.EXPIRE_LOCAL_DESTROY, key, null,
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
  protected boolean localInvalidate() throws EntryNotFoundException
  {
    RegionEntry re = getCheckedRegionEntry();
    Object key = re.getKey();
    LocalRegion lr = getLocalRegion();
    EntryEventImpl event = EntryEventImpl.create(lr,
        Operation.EXPIRE_LOCAL_INVALIDATE, key, null,
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
  final protected void reschedule() throws CacheException
  {
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
  protected void addExpiryTask() throws EntryNotFoundException
  {
    getLocalRegion().addExpiryTask(getCheckedRegionEntry());
  }

  @Override
  public String toString()
  {
    String result = super.toString();
    RegionEntry re = this.re;
    if (re != null) {
      result += ", " + re.getKey();
    }
    return result;
  }

  @Override
  protected void performTimeout() throws CacheException
  {
    // remove the task from the region's map first thing
    // so the next call to addExpiryTaskIfAbsent will
    // add a new task instead of doing nothing, which would
    // erroneously cancel expiration for this key.
    getLocalRegion().cancelExpiryTask(this.re);
    getLocalRegion().performExpiryTimeout(this);
  }
  
  @Override
  public boolean isPending() {
    RegionEntry re = this.re;
    if(re == null) {
      return false;
    }
    if (re.isDestroyedOrRemoved()) {
      return false;
    }
    ExpirationAction action = getAction();
    if (action == null) {
      return false;
    }
    if((action.isInvalidate() || action.isLocalInvalidate()) && re.isInvalid()) {
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
    }
    else
    if (idle != 0 && idle < ttl) {
      action = getIdleAttributes().getAction();
    }
    else {
      action = getTTLAttributes().getAction();
    }
    return action;
  }

  /**
   * Called by LocalRegion#performExpiryTimeout
   */
  @Override
  protected void basicPerformTimeout(boolean isPending) throws CacheException
  {
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
    }
    else
    if (idle != 0 && idle < ttl) {
      action = getIdleAttributes().getAction();
    }
    else {
      action = getTTLAttributes().getAction();
    }
    // if global scope get distributed lock for destroy and invalidate actions
    if (getLocalRegion().getScope().isGlobal()
        && (action.isDestroy() || action.isInvalidate())) {
      Lock lock = getLocalRegion().getDistributedLock(getCheckedRegionEntry().getKey());
      lock.lock();
      try {
        long expTime = getExpirationTime();
        if (expTime == 0L) {
          return;
        }
        if (getNow() >= expTime) {
          if (logger.isTraceEnabled()) {
            // NOTE: original finer message used this.toString() twice
            logger.trace("{}.performTimeout().getExpirationTime() is {}; {}.expire({}). ttlExpiration: {}, idleExpiration: {}, ttlAttrs: {}, idleAttrs: {} action is: {}",
                this, expTime, this, action, ttl, idle, getTTLAttributes(), getIdleAttributes());
          }
          expire(action, isPending);
          return;
        }
      }
      finally {
        lock.unlock();
      }
    }
    else {
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
    }
    return superCancel;
  }

}
