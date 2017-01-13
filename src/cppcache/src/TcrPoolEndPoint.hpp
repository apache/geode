/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TCR_POOL_ENDPOINT__
#define __TCR_POOL_ENDPOINT__
#include "TcrEndpoint.hpp"
#include "PoolStatistics.hpp"
namespace gemfire {
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
}
#endif
