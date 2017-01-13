/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/Region.hpp>
#include <gfcpp/CacheWriter.hpp>
#include <gfcpp/EntryEvent.hpp>
#include <gfcpp/RegionEvent.hpp>

namespace gemfire {

CacheWriter::CacheWriter() {}

CacheWriter::~CacheWriter() {}

void CacheWriter::close(const RegionPtr& region) {}

bool CacheWriter::beforeUpdate(const EntryEvent& event) { return true; }

bool CacheWriter::beforeCreate(const EntryEvent& event) { return true; }

bool CacheWriter::beforeDestroy(const EntryEvent& event) { return true; }

bool CacheWriter::beforeRegionDestroy(const RegionEvent& event) { return true; }
bool CacheWriter::beforeRegionClear(const RegionEvent& event) { return true; }
}  // namespace gemfire
