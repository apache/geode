#pragma once

#ifndef GEODE_TCRPOOLENDPOINT_H_
#define GEODE_TCRPOOLENDPOINT_H_

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
#include "TcrEndpoint.hpp"
#include "PoolStatistics.hpp"
namespace apache {
namespace geode {
namespace client {
class ThinClientPoolDM;
class TcrPoolEndPoint : public TcrEndpoint {
 public:
  TcrPoolEndPoint(const std::string& name, CacheImpl* cache,
                  ACE_Semaphore& failoverSema, ACE_Semaphore& cleanupSema,
                  ACE_Semaphore& redundancySema, ThinClientPoolDM* dm);
  virtual ThinClientPoolDM* getPoolHADM();

  virtual bool checkDupAndAdd(EventIdPtr eventid);
  virtual void processMarker();
  virtual QueryServicePtr getQueryService();
  virtual void sendRequestForChunkedResponse(const TcrMessage& request,
                                             TcrMessageReply& reply,
                                             TcrConnection* conn);
  virtual void closeFailedConnection(TcrConnection*& conn);
  virtual GfErrType registerDM(bool clientNotification,
                               bool isSecondary = false,
                               bool isActiveEndpoint = false,
                               ThinClientBaseDM* distMgr = NULL);
  virtual void unregisterDM(bool clientNotification,
                            ThinClientBaseDM* distMgr = NULL,
                            bool checkQueueHosted = false);
  using TcrEndpoint::handleIOException;
  virtual bool handleIOException(const std::string& message,
                                 TcrConnection*& conn, bool isBgThread = false);
  void handleNotificationStats(int64 byteLength);
  virtual ~TcrPoolEndPoint() { m_dm = NULL; }
  virtual bool isMultiUserMode();

 protected:
  virtual void closeNotification();
  virtual void triggerRedundancyThread();

 private:
  ThinClientPoolDM* m_dm;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_TCRPOOLENDPOINT_H_
