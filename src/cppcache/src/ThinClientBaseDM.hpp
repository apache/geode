/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __THINCLIENT_BASE_DISTRIBUTION_MANAGER_HPP__
#define __THINCLIENT_BASE_DISTRIBUTION_MANAGER_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include "TcrConnectionManager.hpp"
#include "TcrEndpoint.hpp"
#include <vector>

namespace gemfire {

/**
 * @brief Distribute data between caches
 */
class TcrMessage;
class ThinClientRegion;

class ThinClientBaseDM {
 public:
  ThinClientBaseDM(TcrConnectionManager& connManager, ThinClientRegion* region);
  virtual ~ThinClientBaseDM() = 0;

  virtual void init();
  virtual void destroy(bool keepalive = false);

  virtual GfErrType sendSyncRequest(TcrMessage& request, TcrMessageReply& reply,
                                    bool attemptFailover = true,
                                    bool isBGThrad = false) = 0;

  virtual GfErrType sendSyncRequestRegisterInterest(
      TcrMessage& request, TcrMessageReply& reply, bool attemptFailover = true,
      ThinClientRegion* theRegion = NULL, TcrEndpoint* endpoint = NULL);

  virtual GfErrType sendSyncRequestRegisterInterestEP(TcrMessage& request,
                                                      TcrMessageReply& reply,
                                                      bool attemptFailover,
                                                      TcrEndpoint* endpoint) {
    return GF_NOERR;
  }

  virtual void failover();

  virtual void acquireFailoverLock(){};
  virtual void releaseFailoverLock(){};
  virtual void acquireRedundancyLock(){};
  virtual void releaseRedundancyLock(){};
  virtual void triggerRedundancyThread(){};
  virtual bool isSecurityOn();

  virtual bool isMultiUserMode() { return false; }

  virtual void beforeSendingRequest(const TcrMessage& request,
                                    TcrConnection* conn);
  virtual void afterSendingRequest(const TcrMessage& request,
                                   TcrMessageReply& reply, TcrConnection* conn);

  virtual GfErrType registerInterestForRegion(TcrEndpoint* ep,
                                              const TcrMessage* request,
                                              TcrMessageReply* reply) {
    return GF_NOERR;
  }
  inline static bool isFatalError(GfErrType err) {
    return (err == GF_MSG || err == GF_CACHESERVER_EXCEPTION ||
            err == GF_NOT_AUTHORIZED_EXCEPTION ||
            err == GF_AUTHENTICATION_REQUIRED_EXCEPTION ||
            err == GF_AUTHENTICATION_FAILED_EXCEPTION ||
            err == GF_CACHE_LOCATOR_EXCEPTION);
  }

  inline static bool isFatalClientError(GfErrType err) {
    return (err == GF_NOT_AUTHORIZED_EXCEPTION ||
            err == GF_AUTHENTICATION_REQUIRED_EXCEPTION ||
            err == GF_AUTHENTICATION_FAILED_EXCEPTION ||
            err == GF_CACHE_LOCATOR_EXCEPTION);
  }

  // add a new chunk to the queue
  void queueChunk(TcrChunkedContext* chunk);

  virtual bool isEndpointAttached(TcrEndpoint* ep) { return false; };

  static GfErrType sendRequestToEndPoint(const TcrMessage& request,
                                         TcrMessageReply& reply,
                                         TcrEndpoint* currentEndpoint);

  virtual GfErrType sendRequestToEP(const TcrMessage& request,
                                    TcrMessageReply& reply,
                                    TcrEndpoint* currentEndpoint) = 0;

  virtual TcrEndpoint* getActiveEndpoint() { return NULL; }

  virtual bool checkDupAndAdd(EventIdPtr eventid) {
    return m_connManager.checkDupAndAdd(eventid);
  }

  virtual ACE_Recursive_Thread_Mutex* getRedundancyLock() {
    return m_connManager.getRedundancyLock();
  }

  static bool isDeltaEnabledOnServer() { return s_isDeltaEnabledOnServer; }

  inline static void setDeltaEnabledOnServer(bool isDeltaEnabledOnServer) {
    s_isDeltaEnabledOnServer = isDeltaEnabledOnServer;
    LOGFINE("Delta enabled on server: %s",
            s_isDeltaEnabledOnServer ? "true" : "false");
  }
  TcrConnectionManager& getConnectionManager() { return m_connManager; }
  virtual size_t getNumberOfEndPoints() const { return 0; }
  bool isNotAuthorizedException(const char* exceptionMsg) {
    if (exceptionMsg != NULL &&
        strstr(exceptionMsg,
               "org.apache.geode.security.NotAuthorizedException") != NULL) {
      LOGDEBUG(
          "isNotAuthorizedException() An exception (%s) happened at remote "
          "server.",
          exceptionMsg);
      return true;
    }
    return false;
  }
  bool isPutAllPartialResultException(const char* exceptionMsg) {
    if (exceptionMsg != NULL &&
        strstr(
            exceptionMsg,
            "org.apache.geode.internal.cache.PutAllPartialResultException") !=
            NULL) {
      LOGDEBUG(
          "isNotAuthorizedException() An exception (%s) happened at remote "
          "server.",
          exceptionMsg);
      return true;
    }
    return false;
  }

 protected:
  bool isAuthRequireException(const char* exceptionMsg) {
    if (exceptionMsg != NULL &&
        strstr(exceptionMsg,
               "org.apache.geode.security.AuthenticationRequiredException") !=
            NULL) {
      LOGDEBUG(
          "isAuthRequireExcep() An exception (%s) happened at remote server.",
          exceptionMsg);
      return true;
    }
    return false;
  }

  ThinClientRegion* m_region;

  // methods for the chunk processing thread
  int processChunks(volatile bool& isRunning);
  void startChunkProcessor();
  void stopChunkProcessor();

 private:
  // Disallow copy constructor and assignment operator.
  ThinClientBaseDM(const ThinClientBaseDM&);
  ThinClientBaseDM& operator=(const ThinClientBaseDM&);

 protected:
  static bool unrecoverableServerError(const char* exceptStr);
  static bool nonFatalServerError(const char* exceptStr);
  static GfErrType handleEPError(TcrEndpoint* ep, TcrMessageReply& reply,
                                 GfErrType error);

  TcrConnectionManager& m_connManager;
  // flag to indicate whether initialization completed successfully
  bool m_initDone;
  bool m_clientNotification;

  Queue<TcrChunkedContext> m_chunks;
  GF_TASK_T<ThinClientBaseDM>* m_chunkProcessor;

 private:
  static volatile bool s_isDeltaEnabledOnServer;
  static const char* NC_ProcessChunk;
};

}  // namespace gemfire

#endif  // __THINCLIENT_DISTRIBUTION_MANAGER_HPP__
