/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TCR_ENDPOINT_HPP__
#define __TCR_ENDPOINT_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include <string>
#include <list>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Semaphore.h>
#include <gfcpp/gf_base.hpp>
#include "FairQueue.hpp"
#include "Set.hpp"
#include "TcrConnection.hpp"
#include "GF_TASK_T.hpp"
#include "SpinLock.hpp"

namespace gemfire {
class ThinClientRegion;
class TcrMessage;
class ThinClientBaseDM;
class CacheImpl;
class ThinClientPoolHADM;
class ThinClientPoolDM;
class CPPCACHE_EXPORT TcrEndpoint {
 public:
  TcrEndpoint(
      const std::string& name, CacheImpl* cache, ACE_Semaphore& failoverSema,
      ACE_Semaphore& cleanupSema, ACE_Semaphore& redundancySema,
      ThinClientBaseDM* dm = NULL,
      bool isMultiUserMode = false);  // TODO: need to look for endpoint case

  /* adongre
   * CID 29000: Non-virtual destructor (VIRTUAL_DTOR)
   */
  virtual ~TcrEndpoint();

  virtual GfErrType registerDM(bool clientNotification,
                               bool isSecondary = false,
                               bool isActiveEndpoint = false,
                               ThinClientBaseDM* distMgr = NULL);
  // GfErrType registerPoolDM( bool isSecondary, ThinClientPoolHADM* poolDM );

  virtual void unregisterDM(bool clientNotification,
                            ThinClientBaseDM* distMgr = NULL,
                            bool checkQueueHosted = false);
  // void unregisterPoolDM(  );

  void pingServer(ThinClientPoolDM* poolDM = NULL);
  int receiveNotification(volatile bool& isRunning);
  GfErrType send(const TcrMessage& request, TcrMessageReply& reply);
  GfErrType sendRequestConn(const TcrMessage& request, TcrMessageReply& reply,
                            TcrConnection* conn, std::string& failReason);
  GfErrType sendRequestWithRetry(const TcrMessage& request,
                                 TcrMessageReply& reply, TcrConnection*& conn,
                                 bool& epFailure, std::string& failReason,
                                 int maxSendRetries, bool useEPPool,
                                 int64_t requestedTimeout,
                                 bool isBgThread = false);
  GfErrType sendRequestConnWithRetry(const TcrMessage& request,
                                     TcrMessageReply& reply,
                                     TcrConnection*& conn,
                                     bool isBgThread = false);

  void stopNotifyReceiverAndCleanup();
  void stopNoBlock();

  bool inline connected() const { return m_connected; }

  int inline numRegions() const { return m_numRegions; }

  void inline setNumRegions(int numRegions) { m_numRegions = numRegions; }

  inline const std::string& name() const { return m_name; }

  //  setConnectionStatus is now a public method, as it is used by
  //  TcrDistributionManager.
  void setConnectionStatus(bool status);

  inline const int getNumRegionListeners() const { return m_numRegionListener; }

  // TODO: for single user mode only
  void setUniqueId(int64_t uniqueId) {
    LOGDEBUG("tcrEndpoint:setUniqueId:: %d ", uniqueId);
    m_isAuthenticated = true;
    m_uniqueId = uniqueId;
  }

  int64_t getUniqueId() {
    LOGDEBUG("tcrEndpoint:getUniqueId:: %d ", m_uniqueId);
    return m_uniqueId;
  }

  bool isAuthenticated() { return m_isAuthenticated; }

  void setAuthenticated(bool flag) { m_isAuthenticated = false; }

  virtual bool isMultiUserMode();
  /*{
    if(m_baseDM != NULL)
      return this->m_baseDM->isMultiUserMode();
    else
      return false;
  }*/

  void authenticateEndpoint(TcrConnection*& conn);

  ServerQueueStatus getFreshServerQueueStatus(int32_t& queueSize,
                                              bool addToQueue,
                                              TcrConnection*& statusConn);

  //  TESTING: return true or false
  bool inline getServerQueueStatusTEST() {
    return (m_serverQueueStatus == REDUNDANT_SERVER ||
            m_serverQueueStatus == PRIMARY_SERVER);
  }

  // Get cached server queue props.
  int32_t inline getServerQueueSize() { return m_queueSize; }
  ServerQueueStatus getServerQueueStatus() { return m_serverQueueStatus; }

  // Set server queue props.
  void setServerQueueStatus(ServerQueueStatus queueStatus, int32_t queueSize);

  GfErrType createNewConnection(
      TcrConnection*& newConn, bool isClientNotification = false,
      bool isSecondary = false,
      uint32_t connectTimeout = DEFAULT_CONNECT_TIMEOUT,
      int32_t timeoutRetries = 1, bool sendUpdateNotification = true,
      bool appThreadRequest = false);

  bool needtoTakeConnectLock();
  volatile bool m_needToConnectInLock;
  ACE_Recursive_Thread_Mutex m_connectLock;
  ACE_Condition<ACE_Recursive_Thread_Mutex> m_connectLockCond;

  GfErrType createNewConnectionWL(TcrConnection*& newConn,
                                  bool isClientNotification, bool isSecondary,
                                  uint32_t connectTimeout);

  void setConnected(volatile bool connected = true) { m_connected = connected; }
  virtual ThinClientPoolDM* getPoolHADM() { return NULL; }
  bool isQueueHosted();
  ACE_Recursive_Thread_Mutex& getQueueHostedMutex() {
    return m_notifyReceiverLock;
  }
  /*
  void sendNotificationCloseMsg();
  */

  void setDM(ThinClientBaseDM* dm) {
    LOGDEBUG("tcrendpoint setDM");
    this->m_baseDM = dm;
  }

  int32_t numberOfTimesFailed() { return m_numberOfTimesFailed; }

  void addConnRefCounter(int count) {
    HostAsm::atomicAdd(m_noOfConnRefs, count);
  }

  int getConnRefCounter() { return m_noOfConnRefs; }
  virtual uint16_t getDistributedMemberID() { return m_distributedMemId; }
  virtual void setDistributedMemberID(uint16_t memId) {
    m_distributedMemId = memId;
  }

 protected:
  PropertiesPtr getCredentials();
  volatile int m_maxConnections;
  FairQueue<TcrConnection> m_opConnections;
  virtual bool checkDupAndAdd(EventIdPtr eventid);
  virtual void processMarker();
  virtual void triggerRedundancyThread();
  virtual QueryServicePtr getQueryService();
  virtual void sendRequestForChunkedResponse(const TcrMessage& request,
                                             TcrMessageReply& reply,
                                             TcrConnection* conn);
  virtual void closeFailedConnection(TcrConnection*& conn);
  void closeConnection(TcrConnection*& conn);
  virtual void handleNotificationStats(int64 byteLength){};
  virtual void closeNotification();
  std::list<GF_TASK_T<TcrEndpoint>*> m_notifyReceiverList;
  std::list<TcrConnection*> m_notifyConnectionList;
  TcrConnection* m_notifyConnection;
  GF_TASK_T<TcrEndpoint>* m_notifyReceiver;
  int m_numRegionListener;
  bool m_isQueueHosted;
  ACE_Recursive_Thread_Mutex m_notifyReceiverLock;
  virtual bool handleIOException(const std::string& message,
                                 TcrConnection*& conn, bool isBgThread = false);

 private:
  int64_t m_uniqueId;
  bool m_isAuthenticated;
  ACE_Recursive_Thread_Mutex m_endpointAuthenticationLock;
  volatile bool m_msgSent;
  volatile bool m_pingSent;
  int32_t m_numberOfTimesFailed;
  bool m_isMultiUserMode;

  bool compareTransactionIds(int32_t reqTransId, int32_t replyTransId,
                             std::string& failReason, TcrConnection* conn);
  void closeConnections();
  void setRetryAndTimeout(const TcrMessage& request, int& maxSendRetries,
                          uint32_t& requestedTimeout);

  std::string m_name;
  ACE_Recursive_Thread_Mutex m_connectionLock;
  volatile bool m_connected;
  bool m_isActiveEndpoint;
  int m_numRegions;
  Set<uint16_t> m_ports;
  int m_pingTimeouts;

  int m_notifyCount;

  CacheImpl* m_cache;
  ACE_Semaphore& m_failoverSema;
  ACE_Semaphore& m_cleanupSema;
  ACE_Semaphore m_notificationCleanupSema;
  ACE_Semaphore& m_redundancySema;

  std::list<ThinClientBaseDM*> m_distMgrs;
  ACE_Recursive_Thread_Mutex m_distMgrsLock;

  uint32_t m_dupCount;

  //  TESTING: Durable clients - flag that indicates whether endpoint made
  //  connection to server that has HA queue.
  ServerQueueStatus m_serverQueueStatus;
  bool m_isServerQueueStatusSet;
  int32_t m_queueSize;
  // ThinClientPoolDM* m_poolHADM;
  ThinClientBaseDM* m_baseDM;

  // Disallow copy constructor and assignment operator.
  TcrEndpoint(const TcrEndpoint&);
  TcrEndpoint& operator=(const TcrEndpoint&);
  // number of connections to this endpoint
  volatile int32_t m_noOfConnRefs;
  uint16_t m_distributedMemId;

 protected:
  static const char* NC_Notification;
};
}
#endif  // __TCR_ENDPOINT_HPP__
