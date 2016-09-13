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

package org.apache.geode.cache;

/**
 * Class <code>ClientSession</code> represents a client connection on the
 * server. <code>ClientSessions</code> can be used from the cache server to
 * perform interest registrations and unregistrations on behalf of clients.
 * <code>ClientSessions</code> are only available on the cache server.
 * 
 * <p>
 * The ClientSession is often used in conjunction with a callback
 * <code>EntryEvent</code> as shown below.
 * 
 * <pre>
 * String durableClientId = ...; // Some part of the event's key or value would contain this id
 * Cache cache = CacheFactory.getAnyInstance();
 * CacheServer cacheServer = (CacheServer) cache.getCacheServers().iterator().next();
 * ClientSession clientSession = cacheServer.getClientSession(durableClientId);
 * clientSession.registerInterest(event.getRegion().getFullPath(), event.getKey(), InterestResultPolicy.KEYS_VALUES, true);
 * </pre>
 * 
 * @since GemFire 6.0
 * @see org.apache.geode.cache.server.CacheServer#getClientSession(String)
 *      getClientSession
 * @see org.apache.geode.cache.server.CacheServer#getClientSession(org.apache.geode.distributed.DistributedMember)
 *      getClientSession
 *
 */
public interface ClientSession {

  /**
   * Registers interest in a particular region and key
   * 
   * @param regionName
   *          The name of the region in which to register interest
   * @param keyOfInterest
   *          The key on which to register interest
   * @param policy
   *          The {@link org.apache.geode.cache.InterestResultPolicy}. Note:
   *          For the special token 'ALL_KEYS' and lists of keys, values are not
   *          pushed to the client.
   * @param isDurable
   *          Whether the interest is durable
   * @throws IllegalStateException
   *           if this is not the primary server for the given client
   */
  public void registerInterest(String regionName, Object keyOfInterest,
      InterestResultPolicy policy, boolean isDurable);

  /**
   * Registers interest in a particular region and key
   *
   * @param regionName
   *          The name of the region in which to register interest
   * @param keyOfInterest
   *          The key to on which to register interest
   * @param policy
   *          The {@link org.apache.geode.cache.InterestResultPolicy}. Note:
   *          For the special token 'ALL_KEYS' and lists of keys, values are not
   *          pushed to the client.
   * @param isDurable
   *          Whether the interest is durable
   * @param receiveValues
   *          Whether to receive create or update events as invalidates similar
   *          to notify-by-subscription false. The default is true.
   * @throws IllegalStateException
   *          if this is not the primary server for the given client
   * @since GemFire 6.5
   */
  public void registerInterest(String regionName, Object keyOfInterest,
      InterestResultPolicy policy, boolean isDurable, boolean receiveValues);
  
  /**
   * Registers interest in a particular region and regular expression
   *
   * @param regionName
   *          The name of the region in which to register interest
   * @param regex
   *          The regular expression on which to register interest
   * @param isDurable
   *          Whether the interest is durable
   * @throws IllegalStateException
   *          if this is not the primary server for the given client
   */
  public void registerInterestRegex(String regionName, String regex,
      boolean isDurable);
  
  /**
   * Registers interest in a particular region and regular expression
   * 
   * @param regionName
   *          The name of the region in which to register interest
   * @param regex
   *          The regular expression to on which to register interest
   * @param isDurable
   *          Whether the interest is durable
   * @param receiveValues
   *          Whether to receive create or update events as invalidates similar
   *          to notify-by-subscription false. The default is true.
   * @throws IllegalStateException
   *           if this is not the primary server for the given client
   * @since GemFire 6.5
   */
  public void registerInterestRegex(String regionName, String regex,
      boolean isDurable, boolean receiveValues);

  /**
   * Unregisters interest in a particular region and key
   * 
   * @param regionName
   *          The name of the region in which to unregister interest
   * @param keyOfInterest
   *          The key on which to unregister interest
   * @param isDurable
   *          Whether the interest is durable
   * @throws IllegalStateException
   *           if this is not the primary server for the given client
   */
  public void unregisterInterest(String regionName, Object keyOfInterest,
      boolean isDurable);
  
  /**
   * Unregisters interest in a particular region and key
   * 
   * @param regionName
   *          The name of the region in which to unregister interest
   * @param keyOfInterest
   *          The key on which to unregister interest
   * @param isDurable
   *          Whether the interest is durable
   * @param receiveValues
   *          Whether to receive create or update events as invalidates similar
   *          to notify-by-subscription false. The default is true.
   * @throws IllegalStateException
   *           if this is not the primary server for the given client
   * @since GemFire 6.5
   */
  public void unregisterInterest(String regionName, Object keyOfInterest,
      boolean isDurable, boolean receiveValues);

  /**
   * Unregisters interest in a particular region and regular expression
   * 
   * @param regionName
   *          The name of the region in which to unregister interest
   * @param regex
   *          The regular expression on which to unregister interest
   * @param isDurable
   *          Whether the interest is durable
   * @throws IllegalStateException
   *           if this is not the primary server for the given client
   */
  public void unregisterInterestRegex(String regionName, String regex,
      boolean isDurable);

  /**
   * Unregisters interest in a particular region and regular expression
   * 
   * @param regionName
   *          The name of the region in which to unregister interest
   * @param regex
   *          The regular expression on which to unregister interest
   * @param isDurable
   *          Whether the interest is durable
   * @param receiveValues
   *          Whether to receive create or update events as invalidates similar
   *          to notify-by-subscription false. The default is true.
   * @throws IllegalStateException
   *           if this is not the primary server for the given client
   * @since GemFire 6.5
   */
  public void unregisterInterestRegex(String regionName, String regex,
      boolean isDurable, boolean receiveValues);

  /**
   * Returns whether this server is the primary for this client
   *
   * @return whether this server is the primary for this client
   */
  public boolean isPrimary();
}
