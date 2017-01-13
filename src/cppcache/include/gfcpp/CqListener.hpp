#ifndef __GEMFIRE_CQ_LISTENER_H__
#define __GEMFIRE_CQ_LISTENER_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CqEvent.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @cacheserver
 * Querying is only supported for native clients.
 * @endcacheserver
 * @class CqListener CqListener.hpp
 *
 * Application plug-in interface for handling continuous query events after
 * they occur. The listener has two methods, one that is called when there
 * is an event satisfied by the CQ and the other called when there is an
 * error during CQ processing.
 */
class CPPCACHE_EXPORT CqListener : public SharedBase {
 public:
  /**
   * This method is invoked when an event is occurred on the region
   * that satisfied the query condition of this CQ.
   * This event does not contain an error.
   */
  virtual void onEvent(const CqEvent& aCqEvent);

  /**
   * This method is invoked when there is an error during CQ processing.
   * The error can appear while applying query condition on the event.
   * e.g if the event doesn't has attributes as specified in the CQ query.
   * This event does contain an error. The newValue may or may not be
   * available, and will be NULLPTR if not available.
   */
  virtual void onError(const CqEvent& aCqEvent);

  /** Called when the region containing this callback is closed or destroyed,
  * when
  * the cache is closed, or when a callback is removed from a region
  * using an <code>AttributesMutator</code>.
  *
  * <p>Implementations should cleanup any external
  * resources such as database connections. Any runtime exceptions this method
  * throws will be logged.
  *
  * <p>It is possible for this method to be called multiple times on a single
  * callback instance, so implementations must be tolerant of this.
  *
  * @see Cache#close()
  * @see Region#close
  * @see Region#localDestroyRegion()
  * @see Region#destroyRegion()
  * @see AttributesMutator
  */
  virtual void close();

 protected:
  /**
   * @brief constructors
   */
  CqListener();

 private:
  // never implemented.
  CqListener(const CacheListener& other);
  void operator=(const CqListener& other);
};
}  // namespace gemfire

#endif  //#ifndef __GEMFIRE_CQ_LISTENER_H__
