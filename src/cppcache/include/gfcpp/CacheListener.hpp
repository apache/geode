#ifndef __GEMFIRE_CACHELISTENER_H__
#define __GEMFIRE_CACHELISTENER_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "SharedPtr.hpp"

/**
 * @file
 */

namespace gemfire {

class EntryEvent;
class RegionEvent;

/**
 * @class CacheListener CacheListener.hpp
 * An application plug-in that can be installed on a region.
 * Listeners are change notifications that are invoked
 * AFTER the change has occured for region update operations on a client.
 * Listeners also receive notifications when entries in a region are modified.
 * Multiple events can cause concurrent invocation
 * of <code>CacheListener</code> methods.  If event A occurs before event B,
 * there is no guarantee that their corresponding <code>CacheListener</code>
 * method invocations will occur in the same order. Any exceptions thrown by
 * the listener are caught by GemFire and logged. If the exception is due to
 * listener invocation on the same thread where a region operation has been
 * performed, then a <code>CacheListenerException</code> is thrown back to
 * the application. If the exception is for a notification received from
 * server then that is logged and the notification thread moves on to
 * receiving other notifications.
 *
 * There are two cases in which listeners are invoked. The first is when a
 * region modification operation (e.g. put, create, destroy, invalidate)
 * is performed. For this case it is important to ensure that minimal work is
 * done in the listener before returning control back to Gemfire since the
 * operation will block till the listener has not completed. For example,
 * a listener implementation may choose to hand off the event to a thread pool
 * that then processes the event on its thread rather than the listener thread.
 * The second is when notifications are received from java server as a result
 * of region register interest calls (<code>Region::registerKeys</code> etc),
 * or invalidate notifications when notify-by-subscription is false on the
 * server. In this case the methods of <code>CacheListener</code> are invoked
 * asynchronously (i.e. does not block the thread that receives notification
 * messages). Additionally for the latter case of notifications from server,
 * listener is always invoked whether or not local operation is successful
 * e.g. if a destroy notification is received and key does not exist in the
 * region, the listener will still be invoked. This is different from the
 * first case where listeners are invoked only when the region update
 * operation succeeds.
 *
 * @see AttributesFactory::setCacheListener
 * @see RegionAttributes::getCacheListener
 * @see CacheListenerException
 */
class CPPCACHE_EXPORT CacheListener : public SharedBase {
  /**
    * @brief public methods
    */
 public:
  /**
   * @brief destructor
   */
  virtual ~CacheListener();

  /** Handles the event of a new key being added to a region. The entry
   * did not previously exist in this region in the local cache (even with a
   * NULLPTR value).
   *
   * @param event denotes the event object associated with the entry creation
   * This function does not throw any exception.
   * @see Region::create
   * @see Region::put
   * @see Region::get
   */
  virtual void afterCreate(const EntryEvent& event);

  /** Handles the event of an entry's value being modified in a region. This
   * entry
   * previously existed in this region in the local cache, but its previous
   * value
   * may have been NULLPTR.
   *
   * @param event EntryEvent denotes the event object associated with updating
   * the entry
   * @see Region::put
   */
  virtual void afterUpdate(const EntryEvent& event);

  /**
   * Handles the event of an entry's value being invalidated.
   *
   * @param event EntryEvent denotes the event object associated with the entry
   * invalidation
  */
  virtual void afterInvalidate(const EntryEvent& event);

  /**
   * Handles the event of an entry being destroyed.
   *
   * @param event EntryEvent denotes the event object associated with the entry
   * destruction
   * @see Region::destroy
   */
  virtual void afterDestroy(const EntryEvent& event);

  /** Handles the event of a region being invalidated. Events are not
   * invoked for each individual value that is invalidated as a result of
   * the region being invalidated. Each subregion, however, gets its own
   * <code>regionInvalidated</code> event invoked on its listener.
   * @param event RegionEvent denotes the event object associated with the
   * region
   *        invalidation
   * @see Region::invalidateRegion
   */
  virtual void afterRegionInvalidate(const RegionEvent& event);

  /** Handles the event of a region being destroyed. Events are not invoked for
   * each individual entry that is destroyed as a result of the region being
   * destroyed. Each subregion, however, gets its own
   * <code>afterRegionDestroyed</code> event
   * invoked on its listener.
   * @param event RegionEvent denotes the event object associated with the
   * region
   *        destruction
   * @see Region::destroyRegion
   */
  virtual void afterRegionDestroy(const RegionEvent& event);

  /** Handles the event of a region being cleared. Events are not invoked for
   * each individual entry that is removed as a result of the region being
   * cleared. Each subregion, however, gets its own
   * <code>afterRegionClear</code> event
   * invoked on its listener.
   * @param event RegionEvent denotes the event object associated with the
   * region
   *        removal
   * @see Region::clear
   */
  virtual void afterRegionClear(const RegionEvent& event);

  /** Handles the event of a region being live. This event will be invoked
   * after processing all the past messages.
   * Each subregion, however, gets its own <code>afterRegionLive</code> event
   * invoked on its listener.
   * @param event RegionEvent denotes the associated event object
   */
  virtual void afterRegionLive(const RegionEvent& event);

  /** Called when the region containing this callback is destroyed, when
    * the cache is closed.
    *
    * <p>Implementations should clean up any external
    * resources, such as database connections. Any runtime exceptions this
   * method
    * throws will be logged.
    *
    * <p>It is possible for this method to be called multiple times on a single
    * callback instance, so implementations must be tolerant of this.
    *
    * @see Cache::close
    * @see Region::destroyRegion
    */

  virtual void close(const RegionPtr& region);
  /**
  * Called when all the endpoints assosiated with region are down.
  * This will be called when all the endpoints are down for the first time.
  * If endpoints come up and again go down it will be called again.
  * This will also be called when all endpoints are down and region is attached
  * to the pool.
  * @param region RegionPtr denotes the assosiated region.
  */
  virtual void afterRegionDisconnected(const RegionPtr& region);

 protected:
  /**
   * @brief constructors
   */
  CacheListener();

 private:
  // never implemented.
  CacheListener(const CacheListener& other);
  void operator=(const CacheListener& other);
};

}  // namespace gemfire
#endif  // ifndef __GEMFIRE_CACHELISTENER_H__
