/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXState.hpp
 *
 *  Created on: 09-Feb-2011
 *      Author: ankurs
 */

#ifndef TXSTATE_HPP_
#define TXSTATE_HPP_

#include "TXId.hpp"
#include "TXRegionState.hpp"
#include "TransactionalOperation.hpp"
#include <string>

namespace gemfire {
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
}

#endif /* TXSTATE_HPP_ */
