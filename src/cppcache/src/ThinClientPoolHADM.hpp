/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __THIN_CLIENT_POOL_HADM__
#define __THIN_CLIENT_POOL_HADM__
#include "ThinClientPoolDM.hpp"
#include "PoolAttributes.hpp"
#include "TcrConnectionManager.hpp"
#include "ThinClientHARegion.hpp"
//#include "TcrPoolEndPoint.hpp"
namespace gemfire {

class ThinClientPoolHADM : public ThinClientPoolDM {
 public:
  ThinClientPoolHADM(const char* name, PoolAttributesPtr poolAttr,
                     TcrConnectionManager& connManager);

  void init();

  virtual ~ThinClientPoolHADM() { destroy(); }

  virtual GfErrType sendSyncRequest(TcrMessage& request, TcrMessageReply& reply,
                                    bool attemptFailover = true,
                                    bool isBGThread = false);

  bool registerInterestForHARegion(TcrEndpoint* ep, const TcrMessage* request,
                                   ThinClientHARegion& region);

  GfErrType sendSyncRequestRegisterInterestEP(TcrMessage& request,
                                              TcrMessageReply& reply,
                                              bool attemptFailover,
                                              TcrEndpoint* endpoint);

  GfErrType registerInterestAllRegions(TcrEndpoint* ep,
                                       const TcrMessage* request,
                                       TcrMessageReply* reply);

  virtual void destroy(bool keepAlive = false);

  void readyForEvents();

  void sendNotificationCloseMsgs();

  bool checkDupAndAdd(EventIdPtr eventid) {
    return m_redundancyManager->checkDupAndAdd(eventid);
  }

  void processMarker() {
    // also set the static bool m_processedMarker for makePrimary messages
    m_redundancyManager->m_globalProcessedMarker = true;
  }

  void netDown();

  void pingServerLocal();

  virtual void acquireRedundancyLock() {
    m_redundancyManager->acquireRedundancyLock();
  };
  virtual void releaseRedundancyLock() {
    m_redundancyManager->releaseRedundancyLock();
  };
  virtual ACE_Recursive_Thread_Mutex* getRedundancyLock() {
    return &m_redundancyManager->getRedundancyLock();
  }

  GfErrType sendRequestToPrimary(TcrMessage& request, TcrMessageReply& reply) {
    return m_redundancyManager->sendRequestToPrimary(request, reply);
  }

  virtual void triggerRedundancyThread() { m_redundancySema.release(); }

  bool isReadyForEvent() const {
    return m_redundancyManager->isSentReadyForEvents();
  }

 protected:
  virtual GfErrType sendSyncRequestRegisterInterest(
      TcrMessage& request, TcrMessageReply& reply, bool attemptFailover = true,
      ThinClientRegion* region = NULL, TcrEndpoint* endpoint = NULL);

  virtual GfErrType sendSyncRequestCq(TcrMessage& request,
                                      TcrMessageReply& reply);

  virtual bool preFailoverAction();

  virtual bool postFailoverAction(TcrEndpoint* endpoint);

  virtual void startBackgroundThreads();

 private:
  // Disallow copy constructor and assignment operator.
  ThinClientRedundancyManager* m_redundancyManager;
  ThinClientPoolHADM(const ThinClientPoolHADM&);
  ThinClientPoolHADM& operator=(const ThinClientPoolHADM&);
  // const char* m_name; // COVERITY -> 30305 Uninitialized pointer field
  TcrConnectionManager& m_theTcrConnManager;
  ACE_Semaphore m_redundancySema;
  GF_TASK_T<ThinClientPoolHADM>* m_redundancyTask;

  int redundancy(volatile bool& isRunning);
  /*
  void stopNotificationThreads();
  */
  long m_servermonitorTaskId;
  int checkRedundancy(const ACE_Time_Value&, const void*);

  virtual TcrEndpoint* createEP(const char* endpointName) {
    return new TcrPoolEndPoint(endpointName, m_connManager.getCacheImpl(),
                               m_connManager.m_failoverSema,
                               m_connManager.m_cleanupSema, m_redundancySema,
                               this);
  }

  void removeCallbackConnection(TcrEndpoint*);

  std::list<ThinClientRegion*> m_regions;
  ACE_Recursive_Thread_Mutex m_regionsLock;
  void addRegion(ThinClientRegion* theTCR);
  void removeRegion(ThinClientRegion* theTCR);
  void sendNotConMesToAllregions();
  void addDisMessToQueue(ThinClientRegion* theTCR);

  friend class ThinClientHARegion;
  friend class TcrConnectionManager;
  friend class ThinClientRedundancyManager;
  static const char* NC_Redundancy;
};
typedef SharedPtr<ThinClientPoolHADM> ThinClientPoolHADMPtr;
}
#endif
