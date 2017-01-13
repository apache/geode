/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PoolAttributes.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/PoolFactory.hpp>

using namespace gemfire;

PoolAttributes::PoolAttributes()
    : m_isThreadLocalConn(PoolFactory::DEFAULT_THREAD_LOCAL_CONN),
      m_freeConnTimeout(PoolFactory::DEFAULT_FREE_CONNECTION_TIMEOUT),
      m_loadCondInterval(PoolFactory::DEFAULT_LOAD_CONDITIONING_INTERVAL),
      m_sockBufferSize(PoolFactory::DEFAULT_SOCKET_BUFFER_SIZE),
      m_readTimeout(PoolFactory::DEFAULT_READ_TIMEOUT),
      m_minConns(PoolFactory::DEFAULT_MIN_CONNECTIONS),
      m_maxConns(PoolFactory::DEFAULT_MAX_CONNECTIONS),
      m_retryAttempts(PoolFactory::DEFAULT_RETRY_ATTEMPTS),
      m_statsInterval(PoolFactory::DEFAULT_STATISTIC_INTERVAL),
      m_redundancy(PoolFactory::DEFAULT_SUBSCRIPTION_REDUNDANCY),
      m_msgTrackTimeout(
          PoolFactory::DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT),
      m_subsAckInterval(PoolFactory::DEFAULT_SUBSCRIPTION_ACK_INTERVAL),
      m_idleTimeout(PoolFactory::DEFAULT_IDLE_TIMEOUT),
      m_pingInterval(PoolFactory::DEFAULT_PING_INTERVAL),
      m_updateLocatorListInterval(
          PoolFactory::DEFAULT_UPDATE_LOCATOR_LIST_INTERVAL),
      m_subsEnabled(PoolFactory::DEFAULT_SUBSCRIPTION_ENABLED),
      m_multiuserSecurityMode(PoolFactory::DEFAULT_MULTIUSER_SECURE_MODE),
      m_isPRSingleHopEnabled(PoolFactory::DEFAULT_PR_SINGLE_HOP_ENABLED),
      m_serverGrp(PoolFactory::DEFAULT_SERVER_GROUP) {}

PoolAttributesPtr PoolAttributes::clone() {
  PoolAttributesPtr ptr(new PoolAttributes(*this));
  return ptr;
}

/** Return true if all the attributes are equal to those of other. */
bool PoolAttributes::operator==(const PoolAttributes& other) const {
  if (m_isThreadLocalConn != other.m_isThreadLocalConn) return false;
  if (m_freeConnTimeout != other.m_freeConnTimeout) return false;
  if (m_loadCondInterval != other.m_loadCondInterval) return false;
  if (m_sockBufferSize != other.m_sockBufferSize) return false;
  if (m_readTimeout != other.m_readTimeout) return false;
  if (m_minConns != other.m_minConns) return false;
  if (m_maxConns != other.m_maxConns) return false;
  if (m_retryAttempts != other.m_retryAttempts) return false;
  if (m_statsInterval != other.m_statsInterval) return false;
  if (m_redundancy != other.m_redundancy) return false;
  if (m_msgTrackTimeout != other.m_msgTrackTimeout) return false;
  if (m_subsAckInterval != other.m_subsAckInterval) return false;
  if (m_idleTimeout != other.m_idleTimeout) return false;
  if (m_pingInterval != other.m_pingInterval) return false;
  if (m_updateLocatorListInterval != other.m_updateLocatorListInterval) {
    return false;
  }
  if (m_subsEnabled != other.m_subsEnabled) return false;
  if (m_multiuserSecurityMode != other.m_multiuserSecurityMode) return false;
  if (m_isPRSingleHopEnabled != other.m_isPRSingleHopEnabled) return false;

  if (0 !=
      compareStringAttribute(const_cast<char*>(m_serverGrp.c_str()),
                             const_cast<char*>(other.m_serverGrp.c_str()))) {
    return false;
  }

  if (m_initLocList.size() != other.m_initLocList.size()) return false;
  if (m_initServList.size() != other.m_initServList.size()) return false;

  if (!compareVectorOfStrings(m_initLocList, other.m_initLocList)) return false;
  if (!compareVectorOfStrings(m_initServList, other.m_initServList)) {
    return false;
  }

  return true;
}

bool PoolAttributes::compareVectorOfStrings(
    const std::vector<std::string>& thisVector1,
    const std::vector<std::string>& otherVector1) {
  std::vector<std::string>& thisVector =
      *(const_cast<std::vector<std::string>*>(&thisVector1));
  std::vector<std::string>& otherVector =
      *(const_cast<std::vector<std::string>*>(&otherVector1));
  std::vector<std::string>::iterator it;
  std::vector<std::string>::iterator itOther;

  for (it = thisVector.begin(); it < thisVector.end(); it++) {
    bool matched = false;
    std::string thisOne = *it;
    for (itOther = otherVector.begin(); itOther < otherVector.end();
         itOther++) {
      std::string otherOne = *itOther;

      if (0 == compareStringAttribute(thisOne.c_str(), otherOne.c_str())) {
        matched = true;
        break;
      }
    }

    if (!matched) return false;
  }
  return true;
}

int32_t PoolAttributes::compareStringAttribute(const char* attributeA,
                                               const char* attributeB) {
  if (attributeA == NULL && attributeB == NULL) {
    return 0;
  } else if (attributeA == NULL && attributeB != NULL) {
    return -1;
  } else if (attributeA != NULL && attributeB == NULL) {
    return -1;
  }
  return (strcmp(attributeA, attributeB));
}

void PoolAttributes::addLocator(const char* host, int port) {
  if (m_initServList.size()) {
    throw IllegalArgumentException(
        "Cannot add both locators and servers to a pool");
  }
  char buff[128] = {'\0'};
  ACE_OS::snprintf(buff, 128, "%s:%d", host, port);
  m_initLocList.push_back(buff);
}

void PoolAttributes::addServer(const char* host, int port) {
  if (m_initLocList.size()) {
    throw IllegalArgumentException(
        "Cannot add both locators and servers to a pool");
  }
  char buff[128] = {'\0'};
  ACE_OS::snprintf(buff, 128, "%s:%d", host, port);
  m_initServList.push_back(buff);
}
