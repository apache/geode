#pragma once

#ifndef GEODE_GFCPP_DISTRIBUTEDSYSTEM_H_
#define GEODE_GFCPP_DISTRIBUTEDSYSTEM_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "ExceptionTypes.hpp"
#include "Properties.hpp"
#include "VectorT.hpp"

namespace apache {
namespace geode {
namespace client {
/**
 * @class DistributedSystem DistributedSystem.hpp
 * DistributedSystem encapsulates this applications "connection" into the
 * Geode Java servers distributed system. In order to participate in the
 * Geode Java servers distributed system, each application needs to connect to
 * the DistributedSystem.
 * Each application can only be connected to one DistributedSystem.
 */
class SystemProperties;
class DistributedSystemImpl;
class CacheRegionHelper;
class DiffieHellman;
class TcrConnection;

class CPPCACHE_EXPORT DistributedSystem : public SharedBase {
  /**
   * @brief public methods
   */
 public:
  /**
   * Initializes the Native Client system to be able to connect to the
   * Geode Java servers. If the name string is empty, then the default
   * "NativeDS" is used as the name of distributed system.
   * @throws LicenseException if no valid license is found.
   * @throws IllegalStateException if GFCPP variable is not set and
   *   product installation directory cannot be determined
   * @throws IllegalArgument exception if DS name is NULL
   * @throws AlreadyConnectedException if this call has succeeded once before
   *for this process
   **/
  static DistributedSystemPtr connect(const char* name,
                                      const PropertiesPtr& configPtr = NULLPTR);

  /**
   *@brief disconnect from the distributed system
   *@throws IllegalStateException if not connected
   */
  static void disconnect();

  /** Returns the SystemProperties that were used to create this instance of the
   *  DistributedSystem
   * @return  SystemProperties
   */
  static SystemProperties* getSystemProperties();

  /** Returns the name that identifies the distributed system instance
   * @return  name
   */
  virtual const char* getName() const;

  /** Returns  true if connected, false otherwise
   *
   * @return  true if connected, false otherwise
   */
  static bool isConnected();

  /** Returns a pointer to the DistributedSystem instance
   *
   * @return  instance
   */
  static DistributedSystemPtr getInstance();

  /**
   * @brief destructor
   */
  virtual ~DistributedSystem();

 protected:
  /**
   * @brief constructors
   */
  DistributedSystem(const char* name);

 private:
  char* m_name;
  static bool m_connected;
  static DistributedSystemPtr* m_instance_ptr;
  // static DistributedSystemImpl *m_impl;

 public:
  static DistributedSystemImpl* m_impl;
  friend class CacheRegionHelper;
  friend class DistributedSystemImpl;
  friend class TcrConnection;

 private:
  DistributedSystem(const DistributedSystem&);
  const DistributedSystem& operator=(const DistributedSystem&);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_DISTRIBUTEDSYSTEM_H_
