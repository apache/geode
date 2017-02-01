#pragma once

#ifndef GEODE_THINCLIENTSTICKYMANAGER_H_
#define GEODE_THINCLIENTSTICKYMANAGER_H_

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

#include "TssConnectionWrapper.hpp"
#include <algorithm>
#include <vector>
#include <set>
#include <ace/Recursive_Thread_Mutex.h>
namespace apache {
namespace geode {
namespace client {
class ThinClientPoolDM;
class ServerLocation;
class TcrConnection;
class TcrEndpoint;
class ThinClientStickyManager {
 public:
  ThinClientStickyManager(ThinClientPoolDM* poolDM) : m_dm(poolDM) {}
  bool getStickyConnection(TcrConnection*& conn, GfErrType* error,
                           std::set<ServerLocation>& excludeServers,
                           bool forTransaction);
  void setStickyConnection(TcrConnection* conn, bool forTransaction);
  void addStickyConnection(TcrConnection* conn);
  void cleanStaleStickyConnection();

  void closeAllStickyConnections();
  bool canThisConnBeDeleted(TcrConnection* conn);

  void releaseThreadLocalConnection();
  void setSingleHopStickyConnection(TcrEndpoint* ep, TcrConnection*& conn);
  void getSingleHopStickyConnection(TcrEndpoint* ep, TcrConnection*& conn);
  void getAnyConnection(TcrConnection*& conn);

 private:
  static bool isNULL(TcrConnection** conn);
  ThinClientPoolDM* m_dm;
  std::set<TcrConnection**> m_stickyConnList;
  ACE_Recursive_Thread_Mutex m_stickyLock;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_THINCLIENTSTICKYMANAGER_H_
