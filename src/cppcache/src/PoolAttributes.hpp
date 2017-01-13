#ifndef __GEMFIRE_POOL_ATTRIBUTES_HPP__
#define __GEMFIRE_POOL_ATTRIBUTES_HPP__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include <string>
#include <vector>
#include "ace/OS.h"
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedBase.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/ExceptionTypes.hpp>

/**
 * @file
 */

namespace gemfire {
class PoolAttributes : public SharedBase {
 public:
  PoolAttributes();

  int getFreeConnectionTimeout() const { return m_freeConnTimeout; }
  void setFreeConnectionTimeout(int connectionTimeout) {
    m_freeConnTimeout = connectionTimeout;
  }

  int getLoadConditioningInterval() const { return m_loadCondInterval; }
  void setLoadConditioningInterval(int loadConditioningInterval) {
    m_loadCondInterval = loadConditioningInterval;
  }

  int getSocketBufferSize() const { return m_sockBufferSize; }
  void setSocketBufferSize(int bufferSize) { m_sockBufferSize = bufferSize; }

  int getReadTimeout() const { return m_readTimeout; }
  void setReadTimeout(int timeout) { m_readTimeout = timeout; }
  bool getThreadLocalConnectionSetting() { return m_isThreadLocalConn; }
  void setThreadLocalConnectionSetting(bool isThreadLocal) {
    m_isThreadLocalConn = isThreadLocal;
  }
  int getMinConnections() const { return m_minConns; }
  void setMinConnections(int minConnections) { m_minConns = minConnections; }

  int getMaxConnections() const { return m_maxConns; }
  void setMaxConnections(int maxConnections) { m_maxConns = maxConnections; }

  long getIdleTimeout() const { return m_idleTimeout; }
  void setIdleTimeout(long idleTimeout) { m_idleTimeout = idleTimeout; }

  int getRetryAttempts() const { return m_retryAttempts; }
  void setRetryAttempts(int retryAttempts) { m_retryAttempts = retryAttempts; }

  long getPingInterval() const { return m_pingInterval; }

  void setPingInterval(long pingInterval) { m_pingInterval = pingInterval; }

  long getUpdateLocatorListInterval() const {
    return m_updateLocatorListInterval;
  }

  void setUpdateLocatorListInterval(long updateLocatorListInterval) {
    m_updateLocatorListInterval = updateLocatorListInterval;
  }

  int getStatisticInterval() const { return m_statsInterval; }
  void setStatisticInterval(int statisticInterval) {
    m_statsInterval = statisticInterval;
  }

  const char* getServerGroup() const { return m_serverGrp.c_str(); }
  void setServerGroup(const char* group) { m_serverGrp = group; }

  bool getSubscriptionEnabled() const { return m_subsEnabled; }
  void setSubscriptionEnabled(bool enabled) { m_subsEnabled = enabled; }

  int getSubscriptionRedundancy() const { return m_redundancy; }
  void setSubscriptionRedundancy(int redundancy) { m_redundancy = redundancy; }

  int getSubscriptionMessageTrackingTimeout() const {
    return m_msgTrackTimeout;
  }
  void setSubscriptionMessageTrackingTimeout(int messageTrackingTimeout) {
    m_msgTrackTimeout = messageTrackingTimeout;
  }

  int getSubscriptionAckInterval() const { return m_subsAckInterval; }

  bool getPRSingleHopEnabled() const { return m_isPRSingleHopEnabled; }

  void setPRSingleHopEnabled(bool enabled) { m_isPRSingleHopEnabled = enabled; }

  void setSubscriptionAckInterval(int ackInterval) {
    m_subsAckInterval = ackInterval;
  }

  bool getMultiuserSecureModeEnabled() const { return m_multiuserSecurityMode; }
  void setMultiuserSecureModeEnabled(bool multiuserSecureMode) {
    m_multiuserSecurityMode = multiuserSecureMode;
  }

  void addLocator(const char* host, int port);
  void addServer(const char* host, int port);

  PoolAttributesPtr clone();

  /** Return true if all the attributes are equal to those of other. */
  bool operator==(const PoolAttributes& other) const;

 private:
  bool m_isThreadLocalConn;
  int m_freeConnTimeout;
  int m_loadCondInterval;
  int m_sockBufferSize;
  int m_readTimeout;
  int m_minConns;
  int m_maxConns;
  int m_retryAttempts;
  int m_statsInterval;
  int m_redundancy;
  int m_msgTrackTimeout;
  int m_subsAckInterval;

  long m_idleTimeout;
  long m_pingInterval;
  long m_updateLocatorListInterval;

  bool m_subsEnabled;
  bool m_multiuserSecurityMode;
  bool m_isPRSingleHopEnabled;

  std::string m_serverGrp;
  std::vector<std::string> m_initLocList;
  std::vector<std::string> m_initServList;

  static int32_t compareStringAttribute(const char* attributeA,
                                        const char* attributeB);
  static bool compareVectorOfStrings(
      const std::vector<std::string>& thisVector,
      const std::vector<std::string>& otherVector);

  friend class ThinClientPoolDM;
};

};  // namespace gemfire

#endif  // ifndef __GEMFIRE_POOL_ATTRIBUTES_HPP__
