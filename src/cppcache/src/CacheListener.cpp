/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/CacheListener.hpp>
#include <gfcpp/Region.hpp>
#include <gfcpp/EntryEvent.hpp>
#include <gfcpp/RegionEvent.hpp>

namespace gemfire {

CacheListener::CacheListener() {}

CacheListener::~CacheListener() {}

void CacheListener::close(const RegionPtr& region) {}

void CacheListener::afterCreate(const EntryEvent& event) {}

void CacheListener::afterUpdate(const EntryEvent& event) {}

void CacheListener::afterInvalidate(const EntryEvent& event) {}

void CacheListener::afterDestroy(const EntryEvent& event) {}

void CacheListener::afterRegionInvalidate(const RegionEvent& event) {}

void CacheListener::afterRegionDestroy(const RegionEvent& event) {}

void CacheListener::afterRegionClear(const RegionEvent& event) {}

void CacheListener::afterRegionLive(const RegionEvent& event) {}

void CacheListener::afterRegionDisconnected(const RegionPtr& region) {}
}  // namespace gemfire
