/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "NoopAuthInit.hpp"
#include "gfcpp/Properties.hpp"
#include "gfcpp/Log.hpp"
#include "gfcpp/ExceptionTypes.hpp"

namespace gemfire {

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
}  // namespace gemfire
