#ifndef __GEMFIRE_CACHEFACTORY_H__
#define __GEMFIRE_CACHEFACTORY_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "DistributedSystem.hpp"
#include "Cache.hpp"
#include "CacheAttributes.hpp"
#include "PoolFactory.hpp"
/**
 * @file
 */

#define DEFAULT_POOL_NAME "default_gemfireClientPool"

namespace gemfire {

class CppCacheLibrary;
/**
 * @class CacheFactory CacheFactory.hpp
 * Top level class for configuring and using GemFire on a client.This should be
 *called once to create {@link Cache}.
 *<p>
 * For the default values for the pool attributes see {@link PoolFactory}.
 * To create additional {@link Pool}s see {@link PoolManager}
 */
class CPPCACHE_EXPORT CacheFactory : public SharedBase {
 public:
  /**
   * To create the instance of {@link CacheFactory}
   * @param dsProps
   *        Properties which are applicable at client level.
   */
  static CacheFactoryPtr createCacheFactory(
      const PropertiesPtr& dsProps = NULLPTR);

  /**
   * To create the instance of {@link Cache}.
   */
  CachePtr create();

  /**
   * Gets the instance of {@link Cache} produced by an
   * earlier call to {@link CacheFactory::create}.
   * @param system the <code>DistributedSystem</code> the cache was created
   * with.
   * @return the {@link Cache} associated with the specified system.
   * @throws CacheClosedException if a cache has not been created
   * or the created one is {@link Cache::isClosed closed}
   * @throws EntryNotFoundException if a cache with specified system not found
   */
  static CachePtr getInstance(const DistributedSystemPtr& system);

  /**
   * Gets the instance of {@link Cache} produced by an
   * earlier call to {@link CacheFactory::create}, even if it has been closed.
   * @param system the <code>DistributedSystem</code> the cache was created
   * with.
   * @return the {@link Cache} associated with the specified system.
   * @throws CacheClosedException if a cache has not been created
   * @throws EntryNotFoundException if a cache with specified system is not
   * found
   */
  static CachePtr getInstanceCloseOk(const DistributedSystemPtr& system);

  /**
   * Gets an arbitrary open instance of {@link Cache} produced by an
   * earlier call to {@link CacheFactory::create}.
   * @throws CacheClosedException if a cache has not been created
   * or the only created one is {@link Cache::isClosed closed}
   */
  static CachePtr getAnyInstance();

  /** Returns the version of the cache implementation.
   * For the 1.0 release of GemFire, the string returned is <code>1.0</code>.
   * @return the version of the cache implementation as a <code>String</code>
   */
  static const char* getVersion();

  /** Returns the product description string including product name and version.
   */
  static const char* getProductDescription();

  /**
   * Sets the free connection timeout for this pool.
   * If the pool has a max connections setting, operations will block
   * if all of the connections are in use. The free connection timeout
   * specifies how long those operations will block waiting for
   * a free connection before receiving
   * an {@link AllConnectionsInUseException}. If max connections
   * is not set this setting has no effect.
   * @see #setMaxConnections(int)
   * @param connectionTimeout is the connection timeout in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionTimeout</code>
   * is less than or equal to <code>0</code>.
   */
  CacheFactoryPtr setFreeConnectionTimeout(int connectionTimeout);
  /**
   * Sets the load conditioning interval for this pool.
   * This interval controls how frequently the pool will check to see if
   * a connection to a given server should be moved to a different
   * server to improve the load balance.
   * <p>A value of <code>-1</code> disables load conditioning
   * @param loadConditioningInterval is the connection lifetime in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionLifetime</code>
   * is less than <code>-1</code>.
   */
  CacheFactoryPtr setLoadConditioningInterval(int loadConditioningInterval);
  /**
   * Sets the socket buffer size for each connection made in this pool.
   * Large messages can be received and sent faster when this buffer is larger.
   * Larger buffers also optimize the rate at which servers can send events
   * for client subscriptions.
   * @param bufferSize is the size of the socket buffers used for reading and
   * writing on each connection in this pool.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>bufferSize</code>
   * is less than or equal to <code>0</code>.
   */
  CacheFactoryPtr setSocketBufferSize(int bufferSize);

  /**
   * Sets the thread local connections policy for this pool.
   * If <code>true</code> then any time a thread goes to use a connection
   * from this pool it will check a thread local cache and see if it already
   * has a connection in it. If so it will use it. If not it will get one from
   * this pool and cache it in the thread local. This gets rid of thread
   * contention
   * for the connections but increases the number of connections the servers
   * see.
   * <p>If <code>false</code> then connections are returned to the pool as soon
   * as the operation being done with the connection completes. This allows
   * connections to be shared amonst multiple threads keeping the number of
   * connections down.
   * @param threadLocalConnections if <code>true</code> then enable thread local
   * connections.
   * @return a reference to <code>this</code>
   */
  CacheFactoryPtr setThreadLocalConnections(bool threadLocalConnections);

  /**
   * Sets the number of milliseconds to wait for a response from a server before
   * timing out the operation and trying another server (if any are available).
   * @param timeout is the number of milliseconds to wait for a response from a
   * server
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>timeout</code>
   * is less than or equal to <code>0</code>.
   */
  CacheFactoryPtr setReadTimeout(int timeout);

  /**
   * Sets the minimum number of connections to keep available at all times.
   * When the pool is created, it will create this many connections.
   * If <code>0</code> then connections will not be made until an actual
   * operation
   * is done that requires client-to-server communication.
   * @param minConnections is the initial number of connections
   * this pool will create.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>minConnections</code>
   * is less than <code>0</code>.
   */
  CacheFactoryPtr setMinConnections(int minConnections);

  /**
   * Sets the max number of client to server connections that the pool will
   * create. If all of
   * the connections are in use, an operation requiring a client to server
   * connection
   * will block until a connection is available.
   * @see #setFreeConnectionTimeout(int)
   * @param maxConnections is the maximum number of connections in the pool.
   * <code>-1</code> indicates that there is no maximum number of connections
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>maxConnections</code>
   * is less than <code>minConnections</code>.
   */
  CacheFactoryPtr setMaxConnections(int maxConnections);

  /**
   * Sets the amount of time a connection can be idle before expiring the
   * connection.
   * If the pool size is greater than the minimum specified by
   * {@link PoolFactory#setMinConnections(int)}, connections which have been
   * idle
   * for longer than the idleTimeout will be closed.
   * @param idleTimeout is the amount of time in milliseconds that an idle
   * connection
   * should live before expiring. -1 indicates that connections should never
   * expire.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>idleTimout</code>
   * is less than <code>0</code>.
   */
  CacheFactoryPtr setIdleTimeout(long idleTimeout);

  /**
   * Set the number of times to retry a request after timeout/exception.
   * @param retryAttempts is the number of times to retry a request
   * after timeout/exception. -1 indicates that a request should be
   * tried against every available server before failing
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>idleTimout</code>
   * is less than <code>0</code>.
   */
  CacheFactoryPtr setRetryAttempts(int retryAttempts);

  /**
   * The frequency with which servers must be pinged to verify that they are
   * still alive.
   * Each server will be sent a ping every <code>pingInterval</code> if there
   * has not
   * been any other communication with the server.
   *
   * These pings are used by the server to monitor the health of
   * the client. Make sure that the <code>pingInterval</code> is less than the
   * maximum time between pings allowed by the bridge server.
   * @param pingInterval is the amount of time in milliseconds between
   * pings.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>pingInterval</code>
   * is less than <code>0</code>.
   */
  CacheFactoryPtr setPingInterval(long pingInterval);

  /**
   * The frequency with which client updates the locator list. To disable this
   * set its
   * value to 0.
   * @param updateLocatorListInterval is the amount of time in milliseconds
   * between
   * checking locator list at locator.
   */
  CacheFactoryPtr setUpdateLocatorListInterval(long updateLocatorListInterval);

  /**
   * The frequency with which the client statistics must be sent to the server.
   * Doing this allows <code>GFMon</code> to monitor clients.
   * <p>A value of <code>-1</code> disables the sending of client statistics
   * to the server.
   *
   * @param statisticInterval is the amount of time in milliseconds between
   * sends of client statistics to the server.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>statisticInterval</code>
   * is less than <code>-1</code>.
   */
  CacheFactoryPtr setStatisticInterval(int statisticInterval);

  /**
   * Configures the group which contains all the servers that this pool connects
   * to.
   * @param group is the server group that this pool will connect to.
   * If the value is <code>null</code> or <code>""</code> then the pool connects
   * to all servers.
   * @return a reference to <code>this</code>
   */
  CacheFactoryPtr setServerGroup(const char* group);

  /**
   * Adds a locator, given its host and port, to this factory.
   * The locator must be a server locator and will be used to discover other
   * running
   * bridge servers and locators.
   * @param host is the host name or ip address that the locator is listening
   * on.
   * @param port is the port that the locator is listening on.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if the <code>host</code> is an unknown
   * host
   * according to {@link java.net.InetAddress#getByName} or if the port is
   * outside
   * the valid range of [1..65535] inclusive.
   * @throws IllegalStateException if the locator has already been {@link
   * #addServer added} to this factory.
   */
  CacheFactoryPtr addLocator(const char* host, int port);

  /**
   * Adds a server, given its host and port, to this factory.
   * The server must be a bridge server and this client will
   * directly connect to the server without consulting a server locator.
   * @param host is the host name or ip address that the server is listening on.
   * @param port is the port that the server is listening on.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if the <code>host</code> is an unknown
   * host
   * according to {@link java.net.InetAddress#getByName} or if the port is
   * outside
   * the valid range of [1..65535] inclusive.
   * @throws IllegalStateException if the server has already been {@link
   * #addLocator added} to this factory.
   */
  CacheFactoryPtr addServer(const char* host, int port);

  /**
   * If set to <code>true</code> then the created pool will have
   * server-to-client
   * subscriptions enabled.
   * If set to <code>false</code> then all <code>Subscription*</code> attributes
   * are ignored at the time of creation.
   * @return a reference to <code>this</code>
   */
  CacheFactoryPtr setSubscriptionEnabled(bool enabled);
  /**
   * Sets the redundancy level for this pools server-to-client subscriptions.
   * If <code>0</code> then no redundant copies are kept on the servers.
   * Otherwise an effort is made to maintain the requested number of
   * copies of the server-to-client subscriptions. At most, one copy per server
   * is
   *  made up to the requested level.
   * @param redundancy is the number of redundant servers for this client's
   * subscriptions.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>redundancyLevel</code>
   * is less than <code>-1</code>.
   */
  CacheFactoryPtr setSubscriptionRedundancy(int redundancy);
  /**
   * Sets the messageTrackingTimeout attribute which is the time-to-live period,
   * in
   * milliseconds, for subscription events the client has received from the
   * server. It is used
   * to minimize duplicate events.
   * Entries that have not been modified for this amount of time
   * are expired from the list.
   * @param messageTrackingTimeout is the number of milliseconds to set the
   * timeout to.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>messageTrackingTimeout</code>
   * is less than or equal to <code>0</code>.
   */
  CacheFactoryPtr setSubscriptionMessageTrackingTimeout(
      int messageTrackingTimeout);

  /**
   * Sets the is the interval in milliseconds
   * to wait before sending acknowledgements to the bridge server for
   * events received from the server subscriptions.
   *
   * @param ackInterval is the number of milliseconds to wait before sending
   * event
   * acknowledgements.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>ackInterval</code>
   * is less than or equal to <code>0</code>.
   */
  CacheFactoryPtr setSubscriptionAckInterval(int ackInterval);

  /**
   * Sets whether Pool is in multi user secure mode.
   * If its in multiuser mode then app needs to get RegionService instance of
   * Cache, to do the operations on cache.
   * Deafult value is false.
   * @param multiuserAuthentication
   *        to set the pool in multiuser mode.
   * @return a reference to <code>this</code>
   */
  CacheFactoryPtr setMultiuserAuthentication(bool multiuserAuthentication);

  /**
   * By default setPRSingleHopEnabled is true<br>
   * The client is aware of location of partitions on servers hosting
   * {@link Region}s.
   * Using this information, the client routes the client cache operations
   * directly to the server which is hosting the required partition for the
   * cache operation.
   * If setPRSingleHopEnabled is false the client can do an extra hop on servers
   * to go to the required partition for that cache operation.
   * The setPRSingleHopEnabled avoids extra hops only for following cache
   * operations:<br>
   * 1. {@link Region#put(Object, Object)}<br>
   * 2. {@link Region#get(Object)}<br>
   * 3. {@link Region#destroy(Object)}<br>
   * 4. {@link Region#getAll(Object, object)}<br>
   * If true, works best when {@link PoolFactory#setMaxConnections(int)} is set
   * to -1.
   * @param name is boolean whether PR Single Hop optimization is enabled or
   * not.
   * @return a reference to <code>this</code>
   */
  CacheFactoryPtr setPRSingleHopEnabled(bool enabled);

  /**
  * Control whether pdx ignores fields that were unread during deserialization.
  * The default is to preserve unread fields be including their data during
  * serialization.
  * But if you configure the cache to ignore unread fields then their data will
  * be lost
  * during serialization.
  * <P>You should only set this attribute to <code>true</code> if you know this
  * member
  * will only be reading cache data. In this use case you do not need to pay the
  * cost
  * of preserving the unread fields since you will never be reserializing pdx
  * data.
  *
  * @param ignore <code>true</code> if fields not read during pdx
  * deserialization should be ignored;
  * <code>false</code>, the default, if they should be preserved.
  *
  *
  * @return this CacheFactory
  * @since 3.6
  */
  CacheFactoryPtr setPdxIgnoreUnreadFields(bool ignore);

  /** Sets the object preference to PdxInstance type.
  * When a cached object that was serialized as a PDX is read
  * from the cache a {@link PdxInstance} will be returned instead of the actual
  * domain class.
  * The PdxInstance is an interface that provides run time access to
  * the fields of a PDX without deserializing the entire PDX.
  * The PdxInstance implementation is a light weight wrapper
  * that simply refers to the raw bytes of the PDX that are kept
  * in the cache. Using this method applications can choose to
  * access PdxInstance instead of C++ object.
  * <p>Note that a PdxInstance is only returned if a serialized PDX is found in
  * the cache.
  * If the cache contains a deserialized PDX, then a domain class instance is
  * returned instead of a PdxInstance.
  *
  *  @param pdxReadSerialized true to prefer PdxInstance
  *  @return this ClientCacheFactory
  */
  CacheFactoryPtr setPdxReadSerialized(bool pdxReadSerialized);

  /**
   * Sets a gemfire property that will be used when creating the {link @Cache}.
   * @param name the name of the gemfire property
   * @param value the value of the gemfire property
   * @return a reference to <code>this</code>
   * @since 3.5
   */
  CacheFactoryPtr set(const char* name, const char* value);

 private:
  PoolFactoryPtr pf;
  PropertiesPtr dsProp;
  bool ignorePdxUnreadFields;
  bool pdxReadSerialized;

  PoolFactoryPtr getPoolFactory();

  CachePtr create(const char* name, DistributedSystemPtr system = NULLPTR,
                  const char* cacheXml = 0,
                  const CacheAttributesPtr& attrs = NULLPTR);

  static void create_(const char* name, DistributedSystemPtr& system,
                      const char* id_data, CachePtr& cptr,
                      bool ignorePdxUnreadFields, bool readPdxSerialized);

  // no instances allowed
  CacheFactory();
  CacheFactory(const PropertiesPtr dsProps);
  ~CacheFactory();

  PoolPtr determineDefaultPool(CachePtr cachePtr);

  static CachePtr getAnyInstance(bool throwException);
  static GfErrType basicGetInstance(const DistributedSystemPtr& system,
                                    bool closeOk, CachePtr& cptr);

  // Set very first time some creates cache
  static CacheFactoryPtr default_CacheFactory;
  static PoolPtr createOrGetDefaultPool();
  static void* m_cacheMap;
  static void init();
  static void cleanup();
  static void handleXML(CachePtr& cachePtr, const char* cachexml,
                        DistributedSystemPtr& system);
  friend class CppCacheLibrary;
  friend class RegionFactory;
  friend class RegionXmlCreation;
  friend class CacheXmlCreation;
};
};  // namespace gemfire

#endif  // ifndef __GEMFIRE_CACHEFACTORY_H__
