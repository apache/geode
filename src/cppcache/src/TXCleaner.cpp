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
 * TxCleaner.cpp
 *
 *  Created on: Nov 13, 2015
 *      Author: sshcherbakov
 */

#include "TXCleaner.hpp"

namespace apache {
namespace geode {
namespace client {

TXCleaner::TXCleaner(CacheTransactionManagerImpl* cacheTxMgr) {
  m_txStateWrapper = TSSTXStateWrapper::s_geodeTSSTXState;
  m_txState = m_txStateWrapper->getTXState();
  m_cacheTxMgr = cacheTxMgr;
}

TXCleaner::~TXCleaner() {
  clean();
  if (m_txState != NULL) {
    m_txState->releaseStickyConnection();
    delete m_txState;
    m_txState = NULL;
  }
}
void TXCleaner::clean() {
  if (m_txState != NULL && m_txState->getTransactionId().ptr() != NULL) {
    m_cacheTxMgr->removeTx(m_txState->getTransactionId()->getId());
  }
  if (m_txStateWrapper != NULL && m_txState != NULL) {
    m_txStateWrapper->setTXState(NULL);
  }
}

TXState* TXCleaner::getTXState() {
  return (m_txStateWrapper == NULL) ? NULL : m_txStateWrapper->getTXState();
}
}  // namespace client
}  // namespace geode
}  // namespace apache
