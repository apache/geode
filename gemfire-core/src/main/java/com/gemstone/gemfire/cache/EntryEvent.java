/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

//import java.io.*;

/** Contains information about an event affecting an entry, including
 * its identity and the the circumstances of the event.
 * It is passed in to <code>CacheListener</code>, <code>CapacityController</code>, and <code>CacheWriter</code>.
 *
 * @author Eric Zoerner
 *
 *
 * @see CacheListener
 * @see CacheWriter
 * @see RegionEvent
 * @since 3.0
 */
public interface EntryEvent<K,V> extends CacheEvent<K,V> {

  /** Returns the key.
   * @return the key
   */
  public K getKey();
  
  
  /**
   * Returns the value in the cache prior to this event.
   * When passed to an event handler after an event occurs, this value
   * reflects the value that was in the cache in this VM, not necessarily
   * the value that was in the cache VM that initiated the operation.
   * In certain scenarios the old value may no longer be available in which
   * case <code>null</code> is returned.
   * This can happen for disk regions when the old value is on disk only.
   *
   * @return the old value in the cache prior to this event.
   * If the entry did not exist, was invalid, or was not available,
   * then null is returned.
   */
  public V getOldValue();
  
  /**
   * Returns the serialized form of the value in the cache before this event.
   *
   * @return the serialized form of the value in the cache before this event
   * 
   * @since 5.5
   */
  public SerializedCacheValue<V> getSerializedOldValue();

  /**
   * Returns the value in the cache after this event.
   *
   * @return the value in the cache after this event
   */
  public V getNewValue();
  
  /**
   * Returns the serialized form of the value in the cache after this event.
   *
   * @return the serialized form of the value in the cache after this event
   * 
   * @since 5.5
   */
  public SerializedCacheValue<V> getSerializedNewValue();

  // Flag query methods
    
  /** Returns true if this event resulted from a loader running in this cache.
   * Note that this will be true even if the local loader called <code>netSearch</code>.
   *
   * If this event is for a Partitioned Region, then true will be returned if the
   * loader ran in the same VM as where the data is hosted. If true is returned, and {@link CacheEvent#isOriginRemote}
   * is true, it means the data is not hosted locally, but the loader was run local to the data.
   * 
   * @return true if this event resulted from local loader execution
   * @deprecated as of GemFire 5.0, use {@link Operation#isLocalLoad} instead.
   */
  @Deprecated
  public boolean isLocalLoad();
  
  /** Returns true if this event resulted from a loader running that was remote
   * from the cache that requested it, i.e., a netLoad. Note that the cache
   * that requested the netLoad may not be this cache in which case
   * <code>isOriginRemote</code> will also return true.
   * @return true if this event resulted from a netLoad
   * @deprecated as of GemFire 5.0, use {@link Operation#isNetLoad} instead.
   */
  @Deprecated
  public boolean isNetLoad();
  
  /** Returns true if this event resulted from a loader.
   * @return true if isLocalLoad or isNetLoad
   * @deprecated as of GemFire 5.0, use {@link Operation#isLoad} instead.
   */
  @Deprecated
  public boolean isLoad();
  
  /** Returns true if this event resulted from a <code>netSearch</code>. If the <code>netSearch</code>
   * was invoked by a loader however, this will return false and <code>isLocalLoad()</code>
   * or <code>isNetLoad()</code> will return true instead.
   *
   * @return true if this event resulted from a netSearch
   * @deprecated as of GemFire 5.0, use {@link Operation#isNetSearch} instead.
   */
  @Deprecated
  public boolean isNetSearch();
  /**
   * Gets the TransactionId for this EntryEvent.
   * @return the ID of the transaction that performed the operation that
   * generated this event; null if no transaction involved.
   * @since 4.0
   */
  public TransactionId getTransactionId();
  
  /**
   * Returns true if this event originated on a client.
   * 
   * @since 5.1
   * @return true if this event originated on a client.
   * @deprecated as of 5.7 use {@link #hasClientOrigin} instead.
   */
  @Deprecated
  public boolean isBridgeEvent();
  /**
   * Returns true if this event originated on a client.
   * 
   * @since 5.7
   * @return true if this event originated on a client.
   */
  public boolean hasClientOrigin();
  /**
   * Returns <code>true</code> if the old value is "available".
   * Not available means that an old value existed but it could not be obtained
   * or it was deemed too expensive to obtain.
   * Note that {@link #getOldValue} will return <code>null</code> when this
   * method returns <code>false</code>.
   * @since 6.0
   */
  public boolean isOldValueAvailable();
}
