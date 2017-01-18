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

#include <gfcpp/SharedBase.hpp>
#include <HostAsm.hpp>

#include <typeinfo>

namespace apache {
namespace geode {
namespace client {

void SharedBase::preserveSB() const { HostAsm::atomicAdd(m_refCount, 1); }

void SharedBase::releaseSB() const {
  if (HostAsm::atomicAdd(m_refCount, -1) == 0) {
    delete this;
  }
}

// dummy instance to use for NULLPTR
const NullSharedBase* const NullSharedBase::s_instancePtr = NULL;
}  // namespace client
}  // namespace geode
}  // namespace apache
