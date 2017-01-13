#ifndef _GEMFIRE_ENTRYEVENT_HPP_
#define _GEMFIRE_ENTRYEVENT_HPP_
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

/** Represents an entry event affecting an entry, including its identity and the
 * the circumstances of the event. */
class CPPCACHE_EXPORT EntryEvent : public gemfire::SharedBase {
 protected:
  RegionPtr m_region;      /**< Region */
  CacheableKeyPtr m_key;   /**< Cacheable key */
  CacheablePtr m_oldValue; /**< Old value */
  CacheablePtr m_newValue; /**< New value */
  UserDataPtr
      m_callbackArgument; /**< Callback argument for this event, if any. */
  bool m_remoteOrigin;    /**< True if from a remote (non-local) process */

 public:
  /** Constructor, given all values. */
  EntryEvent(const RegionPtr& region, const CacheableKeyPtr& key,
             const CacheablePtr& oldValue, const CacheablePtr& newValue,
             const UserDataPtr& aCallbackArgument, const bool remoteOrigin);

  /** Destructor. */
  virtual ~EntryEvent();

  /** Constructor. */
  EntryEvent();

  /** @return the region this event occurred in. */
  inline RegionPtr getRegion() const { return m_region; }

  /** @return the key this event describes. */
  inline CacheableKeyPtr getKey() const { return m_key; }

  /** If the prior state of the entry was invalid, or non-existent/destroyed,
   * then the old value will be NULLPTR.
   * @return the old value in the cache.
   */
  inline CacheablePtr getOldValue() const { return m_oldValue; }

  /** If the event is a destroy or invalidate operation, then the new value
   * will be NULLPTR.
   * @return the updated value from this event
   */
  inline CacheablePtr getNewValue() const { return m_newValue; }

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
  EntryEvent(const EntryEvent& other);
  void operator=(const EntryEvent& other);
};
}

#endif
