#pragma once

#ifndef GEODE_DISTRIBUTEDSYSTEMIMPL_H_
#define GEODE_DISTRIBUTEDSYSTEMIMPL_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

namespace apache {
namespace geode {
namespace client {
class SystemProperties;

/**
* @class DistributedSystemImpl DistributedSystemImpl.hpp
* A "connection" to a Geode distributed system.
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_DISTRIBUTEDSYSTEMIMPL_H_
