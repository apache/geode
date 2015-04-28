/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.tx.TransactionalOperation;

public interface ServerRegionDataAccess {

  /**
   * Does a get on the server
   * @param key the entry key to do the get on
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   * @param clientEvent the client event, if any, for version propagation
   * @return the entry value found by the get if any
   */
  public abstract Object get(Object key, Object callbackArg, EntryEventImpl clientEvent);

  /**
   * Does a region put on the server
   * @param key the entry key to do the put on
   * @param value the entry value to put
   * @param clientEvent the client event, if any, for eventID and version tag propagation
   * @param op the operation type of this event
   * @param requireOldValue
   * @param expectedOldValue
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   * @param isCreateFwd
   */
  public abstract Object put(Object key, Object value, byte[] deltaBytes, EntryEventImpl clientEvent,
      Operation op, boolean requireOldValue, Object expectedOldValue,
      Object callbackArg, boolean isCreateFwd);


  /**
   * Does a region entry destroy on the server
   * @param key the entry key to do the destroy on
   * @param expectedOldValue the value that must be associated with the entry, or null
   * @param operation the operation being performed (Operation.DESTROY, Operation.REMOVE)
   * @param clientEvent the client event, if any, for version propagation
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public abstract Object destroy(Object key, Object expectedOldValue,
      Operation operation, EntryEventImpl clientEvent, Object callbackArg);

  
  /**
   * Does a region entry invalidate on the server
   * @param event the entryEventImpl that represents the invalidate
   */
  public abstract void invalidate(EntryEventImpl event);
  
  
  /**
   * Does a region clear on the server
   * @param eventId the event id for this clear
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public abstract void clear(EventID eventId, Object callbackArg);


  /**
   * Does a region containsKey on a server
   * @param key the entry key to do the containsKey on
   */
  public abstract boolean containsKey(Object key);

  /**
   * Does a region containsKey on a server
   * @param key the entry key to do the containsKey on
   */
  public abstract boolean containsValueForKey(Object key);
  
  
  /**
   * Does a region containsValue on a server
   * @param value the entry value to search for
   */
  public boolean containsValue(Object value);

  
  /**
   * Does a region keySet on a server
   */
  public abstract Set keySet();

  public abstract VersionedObjectList putAll(Map map, EventID eventId, boolean skipCallbacks, Object callbackArg);

  public abstract VersionedObjectList removeAll(Collection<Object> keys, EventID eventId, Object callbackArg);

  public abstract VersionedObjectList getAll(List keys, Object callback);
  
  public int size();

  /**
   * gets an entry from the server, does not invoke loaders
   * @param key
   * @return an {@link EntrySnapshot} for the given key
   */
  public Entry getEntry(Object key);
//  public boolean containsValue(Object value);
//  public Set entries(boolean recursive) {
//  public void invalidate(Object key) throws TimeoutException,
//  public int size()

  /**
   * returns the name of the region to which this interface provides access
   */
  public String getRegionName();
  
  /**
   * returns the region to which this interface provides access.  This may be
   * null in an admin system
   */
  public Region getRegion();


}