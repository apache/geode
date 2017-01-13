#ifndef __IMPL_DISTRIBUTEDSYSTEM_H__
#define __IMPL_DISTRIBUTEDSYSTEM_H__
/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

/**
* @file
*/

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include <gfcpp/gf_types.hpp>
#include "ace/Recursive_Thread_Mutex.h"
#include "ace/Guard_T.h"
#include "ace/OS.h"
#include <gfcpp/DistributedSystem.hpp>
#include "DiffieHellman.hpp"
#include <string>
#include <map>

#ifdef __linux
#include <sys/prctl.h>
#endif

namespace gemfire {
class SystemProperties;

/**
* @class DistributedSystemImpl DistributedSystemImpl.hpp
* A "connection" to a GemFire distributed system.
* The connection will be through a (host, port) pair.
*/

class DistributedSystemImpl;
typedef SharedPtr<DistributedSystemImpl> DistributedSystemImplPtr;

class CPPCACHE_EXPORT DistributedSystemImpl : public SharedBase {
  /**
  * @brief public methods
  */
 public:
  /*
  * threadname should have less than 16 bytes
  */
  static void setThreadName(const char* tn) {
#ifdef __linux
    int idx = 0;
    while (idx < 16) {
      if (tn[idx++] != '\0') {
        continue;
      } else
        break;
    }
    if (tn[idx - 1] != '\0') {
      // throw exception
      throw IllegalArgumentException("Thread name has more than 15 character.");
    }
    prctl(PR_SET_NAME, tn, 0, 0, 0);
#endif
  }
  /**
  * @brief destructor
  */
  virtual ~DistributedSystemImpl();

  /**
  */
  virtual AuthInitializePtr getAuthLoader();

  /** Retrieve the MemberId used to create this Cache. */
  virtual void disconnect();
  virtual void connect();
  std::string m_name;
  DistributedSystem* m_implementee;

  DiffieHellman m_dh;

  /**
  * @brief constructors
  */
  DistributedSystemImpl(const char* name, DistributedSystem* implementee);

  // acquire/release locks

  static void acquireDisconnectLock();

  static void releaseDisconnectLock();

  /**
   * To connect new appdomain instance
   */
  static void connectInstance();

  /**
   * To disconnect appdomain instance
   */
  static void disconnectInstance();

  /**
  * The current number of connection instances created using
  * connectionOrGetInstance().
  */
  static int currentInstances();

  static void registerCliCallback(int appdomainId,
                                  CliCallbackMethod clicallback);

  static void unregisterCliCallback(int appdomainId);

  static void CallCliCallBack();

 private:
  /**
   * Guard for getAuthLoader()
   */
  ACE_Recursive_Thread_Mutex m_authLock;
  static ACE_Recursive_Thread_Mutex m_cliCallbackLock;
  static volatile bool m_isCliCallbackSet;
  static std::map<int, CliCallbackMethod> m_cliCallbackMap;
};
}  // namespace gemfire

#endif  // ifndef __IMPL_DISTRIBUTEDSYSTEM_H__
