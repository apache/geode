/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _TSS_CONNECTION_WRAPPER_HPP_
#define _TSS_CONNECTION_WRAPPER_HPP_
#include <ace/TSS_T.h>
#include <gfcpp/Pool.hpp>
#include <map>
#include <string>
#include "TcrEndpoint.hpp"

namespace gemfire {
class TcrConnection;
typedef std::map<std::string, TcrConnection*> EpNameVsConnection;

class PoolWrapper {
 private:
  PoolPtr m_pool;
  EpNameVsConnection m_EpnameVsConnection;
  PoolWrapper& operator=(const PoolWrapper&);
  PoolWrapper(const PoolWrapper&);

 public:
  TcrConnection* getSHConnection(TcrEndpoint* ep);
  void setSHConnection(TcrEndpoint* ep, TcrConnection* conn);
  PoolWrapper();
  ~PoolWrapper();
  void releaseSHConnections(PoolPtr pool);
  TcrConnection* getAnyConnection();
};

typedef std::map<std::string, PoolWrapper*> poolVsEndpointConnMap;

class TssConnectionWrapper {
 private:
  TcrConnection* m_tcrConn;
  PoolPtr m_pool;
  poolVsEndpointConnMap m_poolVsEndpointConnMap;
  TssConnectionWrapper& operator=(const TssConnectionWrapper&);
  TssConnectionWrapper(const TssConnectionWrapper&);

 public:
  static ACE_TSS<TssConnectionWrapper> s_gemfireTSSConn;
  TcrConnection* getConnection() { return m_tcrConn; }
  TcrConnection* getSHConnection(TcrEndpoint* ep, const char* poolname);
  void setConnection(TcrConnection* conn, PoolPtr& pool) {
    m_tcrConn = conn;
    m_pool = pool;
  }
  void setSHConnection(TcrEndpoint* ep, TcrConnection* conn);
  TcrConnection** getConnDoublePtr() { return &m_tcrConn; }
  TssConnectionWrapper();
  ~TssConnectionWrapper();
  void releaseSHConnections(PoolPtr p);
  TcrConnection* getAnyConnection(const char* poolname);
};
}
#endif
