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
