#ifndef __GEMFIRE_POOL_HPP__
#define __GEMFIRE_POOL_HPP__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "SharedBase.hpp"
#include "gf_types.hpp"
#include "CacheableBuiltins.hpp"
#include "Cache.hpp"
#include "CacheFactory.hpp"
/**
 * @file
 */

namespace gemfire {
class Cache;
class CacheFactory;
class PoolAttributes;
/**
 * A pool of connections to connect from a client to a set of GemFire Cache
 * Servers.
 * <p>Instances of this interface are created using
 * {@link PoolFactory#create}.
 * <p>Existing instances can be found using {@link PoolFactory#find}
 * and {@link PoolFactory#getAll}.
 * <p>The pool name must be configured
 * on the client regions that will use this pool by calling
 * {@link AttributeFactory#setPoolName}.
 *
 *
 */
class CPPCACHE_EXPORT Pool : public SharedBase {
 public:
  /**
   * Gets the name of the connection pool
   *
   * @return the name of the pool
   * @see PoolFactory#create
   */
  virtual const char* getName() const = 0;

  /**
   * Returns the connection timeout of this pool.
   * @see PoolFactory#setFreeConnectionTimeout
   */
  int getFreeConnectionTimeout() const;
  /**
   * Returns the load conditioning interval of this pool.
   * @see PoolFactory#setLoadConditioningInterval
   */
  int getLoadConditioningInterval() const;
  /**
   * Returns the socket buffer size of this pool.
   * @see PoolFactory#setSocketBufferSize
   */
  int getSocketBufferSize() const;
  /**
   * Returns the read timeout of this pool.
   * @see PoolFactory#setReadTimeout
   */
  int getReadTimeout() const;
  /**
   * Gets the minimum connections for this pool.
   * @see PoolFactory#setMinConnections(int)
   */
  int getMinConnections() const;
  /**
   * Gets the maximum connections for this pool.
   * @see PoolFactory#setMaxConnections(int)
   */
  int getMaxConnections() const;
  /**
   * Gets the idle connection timeout for this pool.
   * @see PoolFactory#setIdleTimeout(long)
   */
  long getIdleTimeout() const;
  /**
   * Gets the ping interval for this pool.
   * @see PoolFactory#setPingInterval(long)
   */
  long getPingInterval() const;
  /**
   * Gets the update locator list interval for this pool.
   * @see PoolFactory#setUpdateLocatorListInterval(long)
   */
  long getUpdateLocatorListInterval() const;
  /**
   * Gets the statistic interval for this pool.
   * @see PoolFactory#setStatisticInterval(int)
   */
  int getStatisticInterval() const;
  /**
   * Gets the retry attempts for this pool.
   * @see PoolFactory#setRetryAttempts(int)
   */
  int getRetryAttempts() const;
  /**
  * Returns the true if a server-to-client subscriptions are enabled on this
  * pool.
  * @see PoolFactory#setSubscriptionEnabled
  */
  bool getSubscriptionEnabled() const;
  /**
   * Returns the subscription redundancy level of this pool.
   * @see PoolFactory#setSubscriptionRedundancy
   */
  int getSubscriptionRedundancy() const;
  /**
   * Returns the subscription message tracking timeout of this pool.
   * @see PoolFactory#setSubscriptionMessageTrackingTimeout
   */
  int getSubscriptionMessageTrackingTimeout() const;
  /**
   * Returns the subscription ack interval of this pool.
   * @see PoolFactory#setSubscriptionAckInterval(int)
   */
  int getSubscriptionAckInterval() const;

  /**
   * Returns the server group of this pool.
   * @see PoolFactory#setServerGroup
   */
  const char* getServerGroup() const;

  /**
   * Returns <code>true</code> if thread local connections are enabled on this
   * pool.
   * @see PoolFactory#setThreadLocalConnections
   */
  bool getThreadLocalConnections() const;

  /**
   * Returns <code>true</code> if multiuser authentication is enabled on this
   * pool.
   * @see PoolFactory#setMultiuserAuthentication
   */
  bool getMultiuserAuthentication() const;

  /**
   * Returns true if single-hop optimisation is enabled on this pool.
   * @see PoolFactory#setPRSingleHopEnabled
   */
  bool getPRSingleHopEnabled() const;

  /**
   * If this pool was configured to use <code>threadlocalconnections</code>,
   * then this method will release the connection cached for the calling thread.
   * The connection will then be available for use by other threads.
   *
   * If this pool is not using <code>threadlocalconnections</code>, this method
   * will have no effect.
   */
  virtual void releaseThreadLocalConnection() = 0;

  /**
   * Returns an unmodifiable list locators that
   * this pool is using. Each locator was either
   * {@link PoolFactory#addLocator added explicitly}
   * when the pool was created or was discovered using the explicit locators.
   * <p> If a pool has no locators, then it cannot discover servers or locators
   * at runtime.
   */
  virtual const CacheableStringArrayPtr getLocators() const = 0;

  /**
   * Returns an unmodifiable list of
   * servers this pool is using. These servers where either
   * {@link PoolFactory#addServer added explicitly}
   * when the pool was created or were discovered using this pools {@link
   * #getLocators locators}.
   */
  virtual const CacheableStringArrayPtr getServers() = 0;

  /**
   * Destroys this pool closing any connections it produced.
   * @param keepAlive defines
   *                whether the server should keep the durable client's
   *                subscriptions alive for the timeout period.
   * @throws IllegalStateException
   *                 if the pool is still in use.
   */
  virtual void destroy(bool keepAlive = false) = 0;

  /**
   * Indicates whether this Pool has been
   * destroyed.
   *
   * @return true if the pool has been destroyed.
   */
  virtual bool isDestroyed() const = 0;

  /**
   * Returns the QueryService for this Pool.
   * The query operations performed using this QueryService will be executed
   * on the servers that are associated with this pool.
   * To perform Query operation on the local cache obtain the QueryService
   * instance from the Cache.
   * @throws unSupported Exception when Pool is in multi user mode.
   *
   * @see Cache#getQueryService
   * @return the QueryService
   */
  virtual QueryServicePtr getQueryService() = 0;

  virtual ~Pool();

  /**
   * Returns the approximate number of pending subscription events maintained at
   * server for this durable client pool at the time it (re)connected to the
   * server. Server would start dispatching these events to this durable client
   * pool when it receives {@link Cache#readyForEvents()} from it.
   * <p>
   * Durable clients can call this method on reconnect to assess the amount of
   * 'stale' data i.e. events accumulated at server while this client was away
   * and, importantly, before calling {@link Cache#readyForEvents()}.
   * <p>
   * Any number of invocations of this method during a single session will
   * return the same value.
   * <p>
   * It may return a zero value if there are no events pending at server for
   * this client pool. A negative value returned tells us that no queue was
   * available at server for this client pool.
   * <p>
   * A value -1 indicates that this client pool reconnected to server after its
   * 'durable-client-timeout' period elapsed and hence its subscription queue at
   * server was removed, possibly causing data loss.
   * <p>
   * A value -2 indicates that this client pool connected to server for the
   * first time.
   *
   * @return int The number of subscription events maintained at server for this
   *         durable client pool at the time this pool (re)connected. A negative
   *         value indicates no queue was found for this client pool.
   * @throws IllegalStateException
   *           If called by a non-durable client or if invoked any time after
   *           invocation of {@link Cache#readyForEvents()}.
   * @since 8.1
   */
  int getPendingEventCount() const;

 protected:
  Pool(PoolAttributesPtr attr);
  PoolAttributesPtr m_attrs;

 private:
  /**
   * Returns the logical instance of cache from pool.
   * Each operation on this cache will use this "credentials"
   *
   * @throws IllegalStateException if cache has not been created or it has been
   * closed.
   *        Or if Pool is not in multiusersecure mode.
   * @returns Logical instance of cache to do operations on behalf of one
   * particular user.
   */
  virtual RegionServicePtr createSecureUserCache(PropertiesPtr credentials);

  Pool(const Pool&);

  friend class PoolFactory;
  friend class CacheFactory;
  friend class Cache;
};
}  // namespace gemfire

#endif  // ifndef __GEMFIRE_POOL_HPP__
