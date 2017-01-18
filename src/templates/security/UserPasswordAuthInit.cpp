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

#include "UserPasswordAuthInit.hpp"
#include "gfcpp/Properties.hpp"
#include "gfcpp/Log.hpp"
#include "gfcpp/ExceptionTypes.hpp"

#define SECURITY_USERNAME "security-username"
#define SECURITY_PASSWORD "security-password"

namespace apache {
namespace geode {
namespace client {

extern "C" {
LIBEXP AuthInitialize* createUserPasswordAuthInitInstance() {
  return new UserPasswordAuthInit();
}
}

PropertiesPtr UserPasswordAuthInit::getCredentials(PropertiesPtr& securityprops,
                                                   const char* server) {
  // LOGDEBUG("UserPasswordAuthInit: inside userPassword::getCredentials");
  CacheablePtr userName;
  if (securityprops == NULLPTR ||
      (userName = securityprops->find(SECURITY_USERNAME)) == NULLPTR) {
    throw AuthenticationFailedException(
        "UserPasswordAuthInit: user name "
        "property [" SECURITY_USERNAME "] not set.");
  }

  PropertiesPtr credentials = Properties::create();
  credentials->insert(SECURITY_USERNAME, userName->toString()->asChar());
  CacheablePtr passwd = securityprops->find(SECURITY_PASSWORD);
  // If password is not provided then use empty string as the password.
  if (passwd == NULLPTR) {
    passwd = CacheableString::create("");
  }
  credentials->insert(SECURITY_PASSWORD, passwd->toString()->asChar());
  // LOGDEBUG("UserPasswordAuthInit: inserted username:password - %s:%s",
  //    userName->toString()->asChar(), passwd->toString()->asChar());
  return credentials;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
