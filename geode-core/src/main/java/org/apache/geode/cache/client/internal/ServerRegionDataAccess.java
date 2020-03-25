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
package org.apache.geode.cache.client.internal;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;

public interface ServerRegionDataAccess {

  /**
   * Does a get on the server
   *
   * @param key the entry key to do the get on
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   * @param clientEvent the client event, if any, for version propagation
   * @return the entry value found by the get if any
   */
  Object get(Object key, Object callbackArg, EntryEventImpl clientEvent);

  /**
   * Does a region put on the server
   *
   * @param key the entry key to do the put on
   * @param value the entry value to put
   * @param clientEvent the client event, if any, for eventID and version tag propagation
   * @param op the operation type of this event
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  Object put(Object key, Object value, byte[] deltaBytes, EntryEventImpl clientEvent, Operation op,
      boolean requireOldValue, Object expectedOldValue, Object callbackArg, boolean isCreateFwd);


  /**
   * Does a region entry destroy on the server
   *
   * @param key the entry key to do the destroy on
   * @param expectedOldValue the value that must be associated with the entry, or null
   * @param operation the operation being performed (Operation.DESTROY, Operation.REMOVE)
   * @param clientEvent the client event, if any, for version propagation
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  Object destroy(Object key, Object expectedOldValue, Operation operation,
      EntryEventImpl clientEvent, Object callbackArg);


  /**
   * Does a region entry invalidate on the server
   *
   * @param event the entryEventImpl that represents the invalidate
   */
  void invalidate(EntryEventImpl event);


  /**
   * Does a region clear on the server
   *
   * @param eventId the event id for this clear
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  void clear(EventID eventId, Object callbackArg);


  /**
   * Does a region containsKey on a server
   *
   * @param key the entry key to do the containsKey on
   */
  boolean containsKey(Object key);

  /**
   * Does a region containsKey on a server
   *
   * @param key the entry key to do the containsKey on
   */
  boolean containsValueForKey(Object key);


  /**
   * Does a region containsValue on a server
   *
   * @param value the entry value to search for
   */
  boolean containsValue(Object value);


  /**
   * Does a region keySet on a server
   */
  Set keySet();

  VersionedObjectList putAll(Map<Object, Object> map, EventID eventId, boolean skipCallbacks,
      Object callbackArg);

  VersionedObjectList removeAll(Collection<Object> keys, EventID eventId, Object callbackArg);

  VersionedObjectList getAll(List keys, Object callback);

  int size();

  /**
   * gets an entry from the server, does not invoke loaders
   *
   * @return an {@link EntrySnapshot} for the given key
   */
  Entry getEntry(Object key);

  /**
   * returns the name of the region to which this interface provides access
   */
  String getRegionName();

  /**
   * returns the region to which this interface provides access. This may be null in an admin system
   */
  Region getRegion();


}
