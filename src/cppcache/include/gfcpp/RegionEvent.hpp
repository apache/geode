#ifndef _GEMFIRE_REGIONEVENT_HPP_
#define _GEMFIRE_REGIONEVENT_HPP_
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "Region.hpp"
#include "CacheableKey.hpp"
#include "UserData.hpp"

/** @file
*/

namespace gemfire {

/** Declares region events.
*/

class CPPCACHE_EXPORT RegionEvent {
 protected:
  const RegionPtr m_region; /**< Region for this event. */
  const UserDataPtr
      m_callbackArgument;    /**< Callback argument for this event, if any. */
  const bool m_remoteOrigin; /**< True if from a remote process. */

 public:
  /** Constructor. */
  RegionEvent();
  /** Constructor, given the values. */
  RegionEvent(const RegionPtr& region, const UserDataPtr& aCallbackArgument,
              const bool remoteOrigin);

  /** Destructor. */
  ~RegionEvent();

  /** Return the region this event occurred in. */
  inline RegionPtr getRegion() const { return m_region; }

  /**
   * Returns the callbackArgument passed to the method that generated
   * this event. See the {@link Region} interface methods that take
   * a callbackArgument parameter.
   */
  inline UserDataPtr getCallbackArgument() const { return m_callbackArgument; }

  /** If the event originated in a remote process, returns true. */
  inline bool remoteOrigin() const { return m_remoteOrigin; }

 private:
  // never implemented.
  RegionEvent(const RegionEvent& other);
  void operator=(const RegionEvent& other);
};
}

#endif
