/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __THINCLIENT_POOL_STICKY_MANAGER__
#define __THINCLIENT_POOL_STICKY_MANAGER__

#include "TssConnectionWrapper.hpp"
#include <algorithm>
#include <vector>
#include <set>
#include <ace/Recursive_Thread_Mutex.h>
namespace gemfire {
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
}
#endif
