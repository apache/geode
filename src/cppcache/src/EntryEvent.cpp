/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/EntryEvent.hpp>
#include <CacheableToken.hpp>

namespace gemfire {
EntryEvent::EntryEvent(const RegionPtr& region, const CacheableKeyPtr& key,
                       const CacheablePtr& oldValue,
                       const CacheablePtr& newValue,
                       const UserDataPtr& aCallbackArgument,
                       const bool remoteOrigin)
    : m_region(region),
      m_key(key),
      m_oldValue(oldValue),
      m_newValue(newValue),
      m_callbackArgument(aCallbackArgument),
      m_remoteOrigin(remoteOrigin) {}

EntryEvent::~EntryEvent() {}

EntryEvent::EntryEvent()
    /* adongre
     * CID 28923: Uninitialized scalar field (UNINIT_CTOR)
     */
    : m_remoteOrigin(false) {}
}  // namespace gemfire
