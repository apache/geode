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
#ifndef __GEMFIRE_IMPL_LRULOCALDESTROYACTION_HPP__
#define __GEMFIRE_IMPL_LRULOCALDESTROYACTION_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include "LRUAction.hpp"
#include "RegionInternal.hpp"
#include "MapEntry.hpp"

namespace apache {
namespace geode {
namespace client {

/**
 * @brief LRUAction for localDestroy.
 */
class CPPCACHE_EXPORT LRULocalDestroyAction : public virtual LRUAction {
 private:
  RegionInternal* m_regionPtr;
  LRUEntriesMap* m_entriesMapPtr;

  LRULocalDestroyAction(RegionInternal* regionPtr, LRUEntriesMap* entriesMapPtr)
      : m_regionPtr(regionPtr), m_entriesMapPtr(entriesMapPtr) {
    m_destroys = true;
  }

 public:
  virtual ~LRULocalDestroyAction() {}

  virtual bool evict(const MapEntryImplPtr& mePtr);

  virtual LRUAction::Action getType() { return LRUAction::LOCAL_DESTROY; }
  friend class LRUAction;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif  //__GEMFIRE_IMPL_LRULOCALDESTROYACTION_HPP__
