/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "UserPasswordAuthInit.hpp"
#include "gfcpp/Properties.hpp"
#include "gfcpp/Log.hpp"
#include "gfcpp/ExceptionTypes.hpp"

#define SECURITY_USERNAME "security-username"
#define SECURITY_PASSWORD "security-password"

namespace gemfire {

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
}  // namespace gemfire
