/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/RegionEvent.hpp>

namespace gemfire {

RegionEvent::RegionEvent(const RegionPtr& region,
                         const UserDataPtr& aCallbackArgument,
                         const bool remoteOrigin)
    : m_region(region),
      m_callbackArgument(aCallbackArgument),
      m_remoteOrigin(remoteOrigin) {}

RegionEvent::RegionEvent() : m_remoteOrigin(false) {}

RegionEvent::~RegionEvent() {}
}  // namespace gemfire
