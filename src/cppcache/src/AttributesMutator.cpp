/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/AttributesMutator.hpp>
#include <RegionInternal.hpp>

namespace gemfire {

AttributesMutator::AttributesMutator(const RegionPtr& region)
    : m_region(region) {}

AttributesMutator::~AttributesMutator() { m_region = NULLPTR; }

/** Sets the idleTimeout duration for region entries.
 * @param idleTimeout the idleTimeout in seconds for entries in this region.
 * @return the previous value.
 * @throw IllegalStateException if the new idleTimeout changes entry expiration
 * from
 *   disabled to enabled or enabled to disabled.
 */
int32_t AttributesMutator::setEntryIdleTimeout(int32_t idleTimeout) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustEntryExpiryDuration(idleTimeout);
}

/** Set the idleTimeout Action for region entries.
 * @param action the idleTimeout ExpirationAction::Action for entries in this
 * region.
 * @return the previous value.
 */
ExpirationAction::Action AttributesMutator::setEntryIdleTimeoutAction(
    ExpirationAction::Action action) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustEntryExpiryAction(action);
}

/** Sets the timeToLive duration for region entries.
 * @param timeToLive the timeToLive in seconds for entries in this region.
 * @return the previous value.
 * @throw IllegalStateException if the new timeToLive changes entry expiration
 * from
 *   disabled to enabled or enabled to disabled.
 */
int32_t AttributesMutator::setEntryTimeToLive(int32_t timeToLive) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustEntryExpiryDuration(timeToLive);
}

/** Set the timeToLive Action for region entries.
 * @param action the timeToLive ExpirationAction::Action for entries in this
 * region.
 * @return the previous value.
 */
ExpirationAction::Action AttributesMutator::setEntryTimeToLiveAction(
    ExpirationAction::Action action) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustEntryExpiryAction(action);
}

/** Sets the idleTimeout duration for the region itself.
 * @param idleTimeout the ExpirationAttributes for this region idleTimeout
 * @return the previous value.
 * @throw IllegalStateException if the new idleTimeout changes region expiration
 * from
 *   disabled to enabled or enabled to disabled.
 */
int32_t AttributesMutator::setRegionIdleTimeout(int32_t idleTimeout) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustRegionExpiryDuration(idleTimeout);
}

/** Set the idleTimeout Action for the region itself.
 * @param action the idleTimeout ExpirationAction::Action for this region.
 * @return the previous value.
 */
ExpirationAction::Action AttributesMutator::setRegionIdleTimeoutAction(
    ExpirationAction::Action action) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustRegionExpiryAction(action);
}

/** Sets the timeToLive duration for the region itself.
 * @param timeToLive the ExpirationAttributes for this region timeToLive
 * @return the previous value.
 * @throw IllegalStateException if the new timeToLive changes region expiration
 * from
 *   disabled to enabled or enabled to disabled.
 */
int32_t AttributesMutator::setRegionTimeToLive(int32_t timeToLive) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustRegionExpiryDuration(timeToLive);
}

/** Set the timeToLive Action for the region itself.
 * @param action the timeToLive ExpirationAction::Action for this region.
 * @return the previous value.
 */
ExpirationAction::Action AttributesMutator::setRegionTimeToLiveAction(
    ExpirationAction::Action action) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustRegionExpiryAction(action);
}

/** Sets the Maximum entry count in the region before LRU eviction.
 * @param entriesLimit the number of entries to allow.
 * @return the previous value.
 * @throw IllegalStateException if the new entriesLimit changes LRU from
 *   disabled to enabled or enabled to disabled.
 */
uint32_t AttributesMutator::setLruEntriesLimit(uint32_t entriesLimit) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  return rImpl->adjustLruEntriesLimit(entriesLimit);
}

void AttributesMutator::setCacheListener(const CacheListenerPtr& aListener) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  rImpl->adjustCacheListener(aListener);
}

void AttributesMutator::setCacheListener(const char* libpath,
                                         const char* factoryFuncName) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  rImpl->adjustCacheListener(libpath, factoryFuncName);
}

void AttributesMutator::setCacheLoader(const CacheLoaderPtr& aLoader) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  rImpl->adjustCacheLoader(aLoader);
}

void AttributesMutator::setCacheLoader(const char* libpath,
                                       const char* factoryFuncName) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  rImpl->adjustCacheLoader(libpath, factoryFuncName);
}

void AttributesMutator::setCacheWriter(const CacheWriterPtr& aWriter) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  rImpl->adjustCacheWriter(aWriter);
}

void AttributesMutator::setCacheWriter(const char* libpath,
                                       const char* factoryFuncName) {
  RegionInternal* rImpl = dynamic_cast<RegionInternal*>(m_region.ptr());
  rImpl->adjustCacheWriter(libpath, factoryFuncName);
}
}  // namespace gemfire
