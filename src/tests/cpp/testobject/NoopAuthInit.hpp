#pragma once

#ifndef APACHE_GEODE_GUARD_c4d769074b0c6edf12cc639d50a34284
#define APACHE_GEODE_GUARD_c4d769074b0c6edf12cc639d50a34284

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

#include "gfcpp/AuthInitialize.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @class NoopAuthInit Implementation NoopAuthInit.hpp
 * NoopAuthInit API for getCredentials.
 *
 * The NoopAuthInit class derives from AuthInitialize base class and
 * implements the getCredential API to get the username and password.
 *
 * To use this class the <c>security-client-auth-library</c>
 * property should be set to the name of the shared library (viz.
 * <code>securityImpl</code>) and the <c>security-client-auth-factory</c>
 * property should be set to the name of the global creation function viz.
 * <code>createNoopAuthInitInstance</code>.
 */
class NoopAuthInit : public AuthInitialize {
  /**
   * @brief public methods
   */
 public:
  /**
   * @brief constructor
   */
  NoopAuthInit() {}

  /**
   * @brief destructor
   */
  ~NoopAuthInit() {}

  /**@brief initialize with the given set of security properties
   * and return the credentials for the client as properties.
   * @param props the set of security properties provided to the
   * <code>DistributedSystem.connect</code> method
   * @param server it is the ID of the current endpoint.
   * The format expected is "host:port".
   * @returns the credentials to be used for the given <code>server</code>
   * @remarks This method can modify the given set of properties. For
   * example it may invoke external agents or even interact with the user.
   */
  PropertiesPtr getCredentials(PropertiesPtr& securityprops,
                               const char* server);

  /**
   * @brief Invoked before the cache goes down.
   */
  void close() { return; }

  /**
   * @brief private members
   */

 private:
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // APACHE_GEODE_GUARD_c4d769074b0c6edf12cc639d50a34284
