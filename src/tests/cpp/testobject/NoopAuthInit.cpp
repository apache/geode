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

#include "NoopAuthInit.hpp"
#include "gfcpp/Properties.hpp"
#include "gfcpp/Log.hpp"
#include "gfcpp/ExceptionTypes.hpp"

namespace apache {
namespace geode {
namespace client {

extern "C" {
LIBEXP AuthInitialize* createNoopAuthInitInstance() {
  LOGINFO("rjk: calling createNoopAuthInitInstance");
  return new NoopAuthInit();
}
}

PropertiesPtr NoopAuthInit::getCredentials(PropertiesPtr& securityprops,
                                           const char* server) {
  LOGINFO("rjk: calling NoopAuthInit::getCredentials");
  PropertiesPtr credentials = Properties::create();
  return credentials;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
