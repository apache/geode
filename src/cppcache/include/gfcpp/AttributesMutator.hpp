#pragma once

#ifndef GEODE_GFCPP_ATTRIBUTESMUTATOR_H_
#define GEODE_GFCPP_ATTRIBUTESMUTATOR_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "ExpirationAction.hpp"
/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

class CacheListener;
class CacheLoader;
class CacheWriter;

/**
 * @class AttributesMutator AttributesMutator.hpp
 *
 * Supports modification of certain region attributes after the region has been
 * created. It is required that the attributes be completely initialized
 * using an {@link AttributesFactory} before creating the region.
 *AttributesMutator
 * can be applied to adjusting and tuning a subset of attributes that are
 *modifiable
 * at runtime.
 *<p>
 * The setter methods all return the previous value of the attribute.
 *
 * @see Region::getAttributesMutator
 * @see RegionAttributes
 * @see AttributesFactory
 */
class CPPCACHE_EXPORT AttributesMutator : public SharedBase {
 private:
  RegionPtr m_region;

 public:
  /** Internal constructor. Use Region::getAttributesMutator() to acquire the
   * mutator for a region. */
  AttributesMutator(const RegionPtr& region);

  virtual ~AttributesMutator();

  /** Sets the idleTimeout duration for region entries.
   * @param idleTimeout the idleTimeout in seconds for entries in this region.
   * @return the previous value.
   * @throw IllegalStateException if the new idleTimeout changes entry
   * expiration from
   *   disabled to enabled or enabled to disabled.
   */
  int32_t setEntryIdleTimeout(int32_t idleTimeout);

  /** Set the idleTimeout Action for region entries.
   * @param action the idleTimeout ExpirationAction::Action for entries in this
   * region.
   * @return the previous value.
   */
  ExpirationAction::Action setEntryIdleTimeoutAction(
      ExpirationAction::Action action);

  /** Sets the timeToLive duration for region entries.
   * @param timeToLive the timeToLive in seconds for entries in this region.
   * @return the previous value.
   * @throw IllegalStateException if the new timeToLive changes entry expiration
   * from
   *   disabled to enabled or enabled to disabled.
   */
  int32_t setEntryTimeToLive(int32_t timeToLive);

  /** Set the timeToLive Action for region entries.
   * @param action the timeToLive ExpirationAction::Action for entries in this
   * region.
   * @return the previous value.
   */
  ExpirationAction::Action setEntryTimeToLiveAction(
      ExpirationAction::Action action);

  /** Sets the idleTimeout duration for the region itself.
   * @param idleTimeout the ExpirationAttributes for this region idleTimeout
   * @return the previous value.
   * @throw IllegalStateException if the new idleTimeout changes region
   * expiration from
   *   disabled to enabled or enabled to disabled.
   */
  int32_t setRegionIdleTimeout(int32_t idleTimeout);

  /** Set the idleTimeout Action for the region itself.
   * @param action the idleTimeout ExpirationAction::Action for this region.
   * @return the previous value.
   */
  ExpirationAction::Action setRegionIdleTimeoutAction(
      ExpirationAction::Action action);

  /** Sets the timeToLive duration for the region itself.
   * @param timeToLive the ExpirationAttributes for this region timeToLive
   * @return the previous value.
   * @throw IllegalStateException if the new timeToLive changes region
   * expiration from
   *   disabled to enabled or enabled to disabled.
   */
  int32_t setRegionTimeToLive(int32_t timeToLive);

  /** Set the timeToLive Action for the region itself.
   * @param action the timeToLive ExpirationAction::Action for this region.
   * @return the previous value.
   */
  ExpirationAction::Action setRegionTimeToLiveAction(
      ExpirationAction::Action action);

  /** Sets the Maximum entry count in the region before LRU eviction.
   * @param entriesLimit the number of entries to allow.
   * @return the previous value.
   * @throw IllegalStateException if the new entriesLimit changes LRU from
   *   disabled to enabled or enabled to disabled.
   */
  uint32_t setLruEntriesLimit(uint32_t entriesLimit);

  /** Sets cache listener for region. The previous cache listener will be
   * replaced with
   * <code>aListener</code>.
   * @param aListener cache listener
   */
  void setCacheListener(const CacheListenerPtr& aListener);

  /** Sets cache listener for region. The previous cache listener will be
   * replaced with
   * a listener created using the factory function provided in the given
   * library.
   * @param libpath path of the library containing cache listener factory
   * function.
   * @param factoryFuncName factory function for creating cache listener.
   */
  void setCacheListener(const char* libpath, const char* factoryFuncName);

  /** Sets cache loader for region. The previous cache loader will be replaced
   * with
   * <code>aLoader</code>.
   * @param aLoader cache loader
   */
  void setCacheLoader(const CacheLoaderPtr& aLoader);

  /** Sets cache loader for region. The previous cache loader will be replaced
   * with
   * a loader created using the factory function provided in the given library.
   * @param libpath path of the library containing cache loader factory
   * function.
   * @param factoryFuncName factory function for creating cache loader.
   */
  void setCacheLoader(const char* libpath, const char* factoryFuncName);

  /** Sets cache writer for region. The previous cache writer will be replaced
   * with
   * <code>aWriter</code>.
   * @param aWriter cache writer
   */
  void setCacheWriter(const CacheWriterPtr& aWriter);

  /** Sets cache writer for region. The previous cache writer will be replaced
   * with
   * a writer created using the factory function provided in the given library.
   * @param libpath path of the library containing cache writer factory
   * function.
   * @param factoryFuncName factory function for creating cache writer.
   */
  void setCacheWriter(const char* libpath, const char* factoryFuncName);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_ATTRIBUTESMUTATOR_H_
