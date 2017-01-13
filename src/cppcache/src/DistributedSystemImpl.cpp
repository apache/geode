/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*
* The implementation of the function behaviors specified in the corresponding
*.hpp file.
*
*========================================================================
*/
#include "DistributedSystemImpl.hpp"
#include <gfcpp/SystemProperties.hpp>

using namespace gemfire;

// guard for connect/disconnect
extern ACE_Recursive_Thread_Mutex* g_disconnectLock;
// tracks the number of times connectOrGetInstance() was invoked
int g_numInstances = 0;

volatile bool DistributedSystemImpl::m_isCliCallbackSet = false;
std::map<int, CliCallbackMethod> DistributedSystemImpl::m_cliCallbackMap;
ACE_Recursive_Thread_Mutex DistributedSystemImpl::m_cliCallbackLock;

DistributedSystemImpl::DistributedSystemImpl(const char* name,
                                             DistributedSystem* implementee)
    : m_name(name == 0 ? "" : name), m_implementee(implementee) {
  g_numInstances = 0;
  if (m_implementee->getSystemProperties()->isDhOn()) {
    // m_dh.initDhKeys(m_implementee->getSystemProperties()->getSecurityProperties());
  }
}

DistributedSystemImpl::~DistributedSystemImpl() {
  if (m_implementee->getSystemProperties()->isDhOn()) {
    // m_dh.clearDhKeys();
  }
  g_numInstances = 0;
  LOGFINE("Destroyed DistributedSystemImpl");
}

AuthInitializePtr DistributedSystemImpl::getAuthLoader() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> authGuard(m_authLock);
  return DistributedSystem::getSystemProperties()->getAuthLoader();
}

void DistributedSystemImpl::connect() {}

void DistributedSystemImpl::disconnect() {
  LOGFINE("DistributedSystemImpl::disconnect done");
}

void DistributedSystemImpl::acquireDisconnectLock() {
  g_disconnectLock->acquire();
}

void DistributedSystemImpl::releaseDisconnectLock() {
  g_disconnectLock->release();
}

int DistributedSystemImpl::currentInstances() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(*g_disconnectLock);

  if (DistributedSystem::getInstance() != NULLPTR &&
      DistributedSystem::getInstance()->getSystemProperties() != NULL &&
      !DistributedSystem::getInstance()
           ->getSystemProperties()
           ->isAppDomainEnabled()) {
    return 0;
  }

  return g_numInstances;
}

void DistributedSystemImpl::connectInstance() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(*g_disconnectLock);

  if (DistributedSystem::getInstance()->getSystemProperties() != NULL &&
      DistributedSystem::getInstance()
          ->getSystemProperties()
          ->isAppDomainEnabled()) {
    g_numInstances++;
  }
}

void DistributedSystemImpl::disconnectInstance() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(*g_disconnectLock);

  if (DistributedSystem::getInstance()->getSystemProperties() != NULL &&
      DistributedSystem::getInstance()
          ->getSystemProperties()
          ->isAppDomainEnabled()) {
    g_numInstances--;
  }
}

void DistributedSystemImpl::CallCliCallBack() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(m_cliCallbackLock);
  if (m_isCliCallbackSet == true) {
    for (std::map<int, CliCallbackMethod>::iterator iter =
             m_cliCallbackMap.begin();
         iter != m_cliCallbackMap.end(); ++iter) {
      (*iter).second();
    }
  }
}

void DistributedSystemImpl::registerCliCallback(int appdomainId,
                                                CliCallbackMethod clicallback) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(m_cliCallbackLock);
  m_cliCallbackMap[appdomainId] = clicallback;
  m_isCliCallbackSet = true;
}

void DistributedSystemImpl::unregisterCliCallback(int appdomainId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(m_cliCallbackLock);
  std::map<int, CliCallbackMethod>::iterator iter =
      m_cliCallbackMap.find(appdomainId);
  if (iter != m_cliCallbackMap.end()) {
    m_cliCallbackMap.erase(iter);
    LOGFINE("Removing cliCallback %d", appdomainId);
  }
}
