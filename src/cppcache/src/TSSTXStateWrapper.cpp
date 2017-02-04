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
/*
 * TSSTXStateWrapper.cpp
 *
 *  Created on: 09-Feb-2011
 *      Author: ankurs
 */

#include "TSSTXStateWrapper.hpp"
#include "TXState.hpp"

namespace apache {
namespace geode {
namespace client {
ACE_TSS<TSSTXStateWrapper> TSSTXStateWrapper::s_geodeTSSTXState;

TSSTXStateWrapper::TSSTXStateWrapper() { m_txState = NULL; }

TSSTXStateWrapper::~TSSTXStateWrapper() {
  if (m_txState) {
    delete m_txState;
    m_txState = NULL;
  }
}
}  // namespace client
}  // namespace geode
}  // namespace apache
