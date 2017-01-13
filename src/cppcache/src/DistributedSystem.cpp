

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 *
 * The implementation of the function behaviors specified in the
 * corresponding .hpp file.
 *
 *========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>

#include <gfcpp/DistributedSystem.hpp>
#include "statistics/StatisticsManager.hpp"
#include <gfcpp/SystemProperties.hpp>

#include <CppCacheLibrary.hpp>
#include <Utils.hpp>
#include <gfcpp/Log.hpp>
#include <gfcpp/CacheFactory.hpp>
#include <ace/OS.h>

#include <ExpiryTaskManager.hpp>
#include <CacheImpl.hpp>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <gfcpp/DataOutput.hpp>
#include <TcrMessage.hpp>
#include <DistributedSystemImpl.hpp>
#include <RegionStats.hpp>
#include <PoolStatistics.hpp>

#include <DiffieHellman.hpp>

#include "version.h"

using namespace gemfire;
using namespace gemfire_statistics;

DistributedSystemPtr* DistributedSystem::m_instance_ptr = NULL;
bool DistributedSystem::m_connected = false;
DistributedSystemImpl* DistributedSystem::m_impl = NULL;

ACE_Recursive_Thread_Mutex* g_disconnectLock = new ACE_Recursive_Thread_Mutex();

namespace {

StatisticsManager* g_statMngr = NULL;

SystemProperties* g_sysProps = NULL;
}

namespace gemfire {
void setLFH() {
#ifdef _WIN32
  static HINSTANCE kernelMod = NULL;
  if (kernelMod == NULL) {
    kernelMod = GetModuleHandle("kernel32");
    if (kernelMod != NULL) {
      typedef BOOL(WINAPI * PHSI)(
          HANDLE HeapHandle, HEAP_INFORMATION_CLASS HeapInformationClass,
          PVOID HeapInformation, SIZE_T HeapInformationLength);
      typedef HANDLE(WINAPI * PGPH)();
      PHSI pHSI = NULL;
      PGPH pGPH = NULL;
      if ((pHSI = (PHSI)GetProcAddress(kernelMod, "HeapSetInformation")) !=
          NULL) {
        // The LFH API is available
        /* Only set LFH for process heap; causes problems in C++ framework if
        set for all heaps
        HANDLE hProcessHeapHandles[1024];
        DWORD dwRet;
        ULONG heapFragValue = 2;

        dwRet= GetProcessHeaps( 1024, hProcessHeapHandles );
        for (DWORD i = 0; i < dwRet; i++)
        {
          HeapSetInformation( hProcessHeapHandles[i],
            HeapCompatibilityInformation, &heapFragValue, sizeof(heapFragValue)
        );
        }
        */
        HANDLE hProcessHeapHandle;
        ULONG heapFragValue = 2;
        if ((pGPH = (PGPH)GetProcAddress(kernelMod, "GetProcessHeap")) !=
            NULL) {
          hProcessHeapHandle = pGPH();
          LOGCONFIG(
              "Setting Microsoft Windows' low-fragmentation heap for use as "
              "the main process heap.");
          pHSI(hProcessHeapHandle, HeapCompatibilityInformation, &heapFragValue,
               sizeof(heapFragValue));
        }
      }
    }
  }
#endif
}
}  // namespace gemfire

DistributedSystem::DistributedSystem(const char* name) : m_name(NULL) {
  LOGDEBUG("DistributedSystem::DistributedSystem");
  if (name != NULL) {
    size_t len = strlen(name) + 1;
    m_name = new char[len];
    ACE_OS::strncpy(m_name, name, len);
  }
  if (strlen(g_sysProps->securityClientDhAlgo()) > 0) {
    DiffieHellman::initOpenSSLFuncPtrs();
  }
}
DistributedSystem::~DistributedSystem() { GF_SAFE_DELETE_ARRAY(m_name); }

DistributedSystemPtr DistributedSystem::connect(
    const char* name, const PropertiesPtr& configPtr) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(*g_disconnectLock);
  setLFH();
  if (m_connected == true) {
    throw AlreadyConnectedException(
        "DistributedSystem::connect: already connected, call getInstance to "
        "get it");
  }

  // make sure statics are initialized.
  if (m_instance_ptr == NULL) {
    m_instance_ptr = new DistributedSystemPtr();
  }
  if (g_sysProps == NULL) {
    g_sysProps = new SystemProperties(configPtr, NULL);
  }
  Exception::setStackTraces(g_sysProps->debugStackTraceEnabled());

  if (name == NULL) {
    delete g_sysProps;
    g_sysProps = NULL;
    throw IllegalArgumentException(
        "DistributedSystem::connect: "
        "name cannot be NULL");
  }
  if (name[0] == '\0') {
    name = "NativeDS";
  }

  // Fix for Ticket#866 on NC OR SR#13306117704
  // Set client name via native client API
  const char* propName = g_sysProps->name();
  if (propName != NULL && strlen(propName) > 0) {
    name = propName;
  }

  // Trigger other library initialization.
  CppCacheLibrary::initLib();

  if (!TcrMessage::init()) {
    TcrMessage::cleanup();
    throw IllegalArgumentException(
        "DistributedSystem::connect: preallocate message buffer failed!");
  }

  const char* logFilename = g_sysProps->logFilename();
  if (logFilename != NULL) {
    try {
      Log::close();
      Log::init(g_sysProps->logLevel(), logFilename,
                g_sysProps->logFileSizeLimit(),
                g_sysProps->logDiskSpaceLimit());
    } catch (const GemfireIOException&) {
      Log::close();
      TcrMessage::cleanup();
      CppCacheLibrary::closeLib();
      delete g_sysProps;
      g_sysProps = NULL;
      *m_instance_ptr = NULLPTR;
      // delete g_disconnectLock;
      throw;
    }
  } else {
    Log::setLogLevel(g_sysProps->logLevel());
  }

  try {
    std::string gfcpp = CppCacheLibrary::getProductDir();
    LOGCONFIG("Using GemFire Native Client Product Directory: %s",
              gfcpp.c_str());
  } catch (const Exception&) {
    LOGERROR(
        "Unable to determine Product Directory. Please set the "
        "GFCPP environment variable.");
    throw;
  }
  // Add version information, source revision, current directory etc.
  LOGCONFIG("Product version: %s", CacheFactory::getProductDescription());
  LOGCONFIG("Source revision: %s", GEMFIRE_SOURCE_REVISION);
  LOGCONFIG("Source repository: %s", GEMFIRE_SOURCE_REPOSITORY);

  ACE_utsname u;
  ACE_OS::uname(&u);
  LOGCONFIG(
      "Running on: SystemName=%s Machine=%s Host=%s Release=%s Version=%s",
      u.sysname, u.machine, u.nodename, u.release, u.version);

#ifdef _WIN32
  const uint32_t pathMax = _MAX_PATH;
#else
  const uint32_t pathMax = PATH_MAX;
#endif
  ACE_TCHAR cwd[pathMax + 1];
  (void)ACE_OS::getcwd(cwd, pathMax);
  LOGCONFIG("Current directory: %s", cwd);
  LOGCONFIG("Current value of PATH: %s", ACE_OS::getenv("PATH"));
#ifndef _WIN32
  const char* ld_libpath = ACE_OS::getenv("LD_LIBRARY_PATH");
  LOGCONFIG("Current library path: %s",
            ld_libpath == NULL ? "NULL" : ld_libpath);
#else
  if (Utils::s_setNewAndDelete) {
    LOGCONFIG("Operators new and delete have been set.");
  }
#endif
  // Log the Gemfire system properties
  g_sysProps->logSettings();

  /* if (strlen(g_sysProps->securityClientDhAlgo())>0) {
     DiffieHellman::initDhKeys(g_sysProps->getSecurityProperties());
   }*/

  DistributedSystemPtr dptr;
  try {
    g_statMngr = StatisticsManager::initInstance(
        g_sysProps->statisticsArchiveFile(),
        g_sysProps->statisticsSampleInterval(), g_sysProps->statisticsEnabled(),
        g_sysProps->statsFileSizeLimit(), g_sysProps->statsDiskSpaceLimit());
  } catch (const NullPointerException&) {
    // close all open handles, delete whatever was newed.
    g_statMngr = NULL;
    //:Merge issue
    /*if (strlen(g_sysProps->securityClientDhAlgo())>0) {
      DiffieHellman::clearDhKeys();
    }*/
    Log::close();
    TcrMessage::cleanup();
    CppCacheLibrary::closeLib();
    delete g_sysProps;
    g_sysProps = NULL;
    *m_instance_ptr = NULLPTR;
    // delete g_disconnectLock;
    throw;
  }
  GF_D_ASSERT(g_statMngr != NULL);

  CacheImpl::expiryTaskManager = new ExpiryTaskManager();
  CacheImpl::expiryTaskManager->begin();

  DistributedSystem* dp = new DistributedSystem(name);
  if (!dp) {
    throw NullPointerException("DistributedSystem::connect: new failed");
  }
  m_impl = new DistributedSystemImpl(name, dp);

  try {
    m_impl->connect();
  } catch (const gemfire::Exception& e) {
    LOGERROR("Exception caught during client initialization: %s",
             e.getMessage());
    std::string msg = "DistributedSystem::connect: caught exception: ";
    msg.append(e.getMessage());
    throw NotConnectedException(msg.c_str());
  } catch (const std::exception& e) {
    LOGERROR("Exception caught during client initialization: %s", e.what());
    std::string msg = "DistributedSystem::connect: caught exception: ";
    msg.append(e.what());
    throw NotConnectedException(msg.c_str());
  } catch (...) {
    LOGERROR("Unknown exception caught during client initialization");
    throw NotConnectedException(
        "DistributedSystem::connect: caught unknown exception");
  }

  m_connected = true;
  dptr = dp;
  *m_instance_ptr = dptr;
  LOGCONFIG("Starting the GemFire Native Client");

  return dptr;
}

/**
 *@brief disconnect from the distributed system
 */
void DistributedSystem::disconnect() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> disconnectGuard(*g_disconnectLock);

  if (!m_connected) {
    throw NotConnectedException(
        "DistributedSystem::disconnect: connect "
        "not called");
  }

  try {
    CachePtr cache = CacheFactory::getAnyInstance();
    if (cache != NULLPTR && !cache->isClosed()) {
      cache->close();
    }
  } catch (const gemfire::Exception& e) {
    LOGWARN("Exception while closing: %s: %s", e.getName(), e.getMessage());
  }

  if (CacheImpl::expiryTaskManager != NULL) {
    CacheImpl::expiryTaskManager->stopExpiryTaskManager();
  }

  if (m_impl) {
    m_impl->disconnect();
    delete m_impl;
    m_impl = NULL;
  }

  LOGFINEST("Deleted DistributedSystemImpl");

  if (strlen(g_sysProps->securityClientDhAlgo()) > 0) {
    //  DistributedSystem::getInstance()->m_dh->clearDhKeys();
  }

  // Clear DH Keys
  /* if (strlen(g_sysProps->securityClientDhAlgo())>0) {
     DiffieHellman::clearDhKeys();
   }*/

  GF_D_ASSERT(!!g_sysProps);
  delete g_sysProps;
  g_sysProps = NULL;

  LOGFINEST("Deleted SystemProperties");

  if (CacheImpl::expiryTaskManager != NULL) {
    delete CacheImpl::expiryTaskManager;
    CacheImpl::expiryTaskManager = NULL;
  }

  LOGFINEST("Deleted ExpiryTaskManager");

  TcrMessage::cleanup();

  LOGFINEST("Cleaned TcrMessage");

  GF_D_ASSERT(!!g_statMngr);
  g_statMngr->clean();
  g_statMngr = NULL;

  LOGFINEST("Cleaned StatisticsManager");

  RegionStatType::clean();

  LOGFINEST("Cleaned RegionStatType");

  PoolStatType::clean();

  LOGFINEST("Cleaned PoolStatType");

  *m_instance_ptr = NULLPTR;

  // Free up library resources
  CppCacheLibrary::closeLib();

  LOGCONFIG("Stopped the GemFire Native Client");

  Log::close();

  m_connected = false;
}

SystemProperties* DistributedSystem::getSystemProperties() {
  return g_sysProps;
}

const char* DistributedSystem::getName() const { return m_name; }

bool DistributedSystem::isConnected() {
  CppCacheLibrary::initLib();
  return m_connected;
}

DistributedSystemPtr DistributedSystem::getInstance() {
  CppCacheLibrary::initLib();
  if (m_instance_ptr == NULL) {
    return NULLPTR;
  }
  return *m_instance_ptr;
}
