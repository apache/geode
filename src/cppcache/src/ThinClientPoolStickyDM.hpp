#pragma once

#ifndef GEODE_THINCLIENTPOOLSTICKYDM_H_
#define GEODE_THINCLIENTPOOLSTICKYDM_H_

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
#include "ThinClientPoolDM.hpp"
#include "ThinClientStickyManager.hpp"
namespace apache {
namespace geode {
namespace client {
class ThinClientPoolStickyDM : public ThinClientPoolDM {
 public:
  ThinClientPoolStickyDM(const char* name, PoolAttributesPtr poolAttrs,
                         TcrConnectionManager& connManager)
      : ThinClientPoolDM(name, poolAttrs, connManager) {
    m_sticky = true;
  }
  virtual ~ThinClientPoolStickyDM() {
    // m_manager->closeAllStickyConnections();
    // delete m_manager; m_manager = NULL;
  }
  virtual bool canItBeDeletedNoImpl(TcrConnection* conn);

 protected:
  virtual void cleanStickyConnections(volatile bool& isRunning);
  virtual TcrConnection* getConnectionFromQueueW(
      GfErrType* error, std::set<ServerLocation>&, bool isBGThread,
      TcrMessage& request, int8_t& version, bool& match, bool& connFound,
      const BucketServerLocationPtr& serverLocation = NULLPTR);
  virtual void putInQueue(TcrConnection* conn, bool isBGThread,
                          bool isTransaction = false);
  virtual void setStickyNull(bool isBGThread);
  virtual bool canItBeDeleted(TcrConnection* conn);
  virtual void releaseThreadLocalConnection();
  virtual void setThreadLocalConnection(TcrConnection* conn);

  // virtual void cleanStickyConnections(volatile bool& isRunning);
};
typedef SharedPtr<ThinClientPoolStickyDM> ThinClientPoolStickyDMPtr;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_THINCLIENTPOOLSTICKYDM_H_
