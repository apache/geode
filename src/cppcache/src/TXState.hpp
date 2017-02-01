#pragma once

#ifndef GEODE_TXSTATE_H_
#define GEODE_TXSTATE_H_

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
 * TXState.hpp
 *
 *  Created on: 09-Feb-2011
 *      Author: ankurs
 */


#include "TXId.hpp"
#include "TXRegionState.hpp"
#include "TransactionalOperation.hpp"
#include <string>

namespace apache {
namespace geode {
namespace client {
class ThinClientPoolDM;
class TXState {
 public:
  TXState(Cache* cache);
  virtual ~TXState();

  TXIdPtr getTransactionId();
  bool isDirty() { return m_dirty; }
  void setDirty() { m_dirty = true; }
  bool isReplay() { return m_replay; }
  bool isPrepared() { return m_prepared; }
  void setPrepared() { m_prepared = true; }
  void recordTXOperation(ServerRegionOperation op, const char* regionName,
                         CacheableKeyPtr key, VectorOfCacheablePtr arguments);
  CacheablePtr replay(bool isRollback);
  void releaseStickyConnection();

  // This variable is used only when the transaction is suspended and resumed.
  // For using this variable somewhere else, care needs to be taken
  std::string getEPStr() { return epNameStr; }
  void setEPStr(std::string ep) { epNameStr = ep; }

  ThinClientPoolDM* getPoolDM() { return m_pooldm; }
  void setPoolDM(ThinClientPoolDM* dm) { m_pooldm = dm; }
  void setSuspendedExpiryTaskId(long suspendedExpiryTaskId) {
    m_suspendedExpiryTaskId = suspendedExpiryTaskId;
  }
  long getSuspendedExpiryTaskId() { return m_suspendedExpiryTaskId; }

 private:
  void startReplay() { m_replay = true; };
  void endReplay() { m_replay = false; };

 private:
  TXIdPtr m_txId;
  /**
   * Used to hand out modification serial numbers used to preserve
   * the order of operation done by this transaction.
   */
  int32_t m_modSerialNum;
  // A map of transaction state by Region
  bool m_closed;
  bool m_dirty;
  bool m_prepared;
  // This variable is used only when the transaction is suspended and resumed.
  // For using this variable somewhere else, care needs to be taken
  std::string epNameStr;
  int32_t nextModSerialNum();
  bool m_replay;
  VectorOfSharedBase m_operations;
  Cache* m_cache;
  ThinClientPoolDM* m_pooldm;
  long m_suspendedExpiryTaskId;
  class ReplayControl {
   public:
    ReplayControl(TXState* txState) : m_txState(txState) {
      m_txState->startReplay();
    };
    virtual ~ReplayControl() { m_txState->endReplay(); };

   private:
    TXState* m_txState;
  };
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_TXSTATE_H_
