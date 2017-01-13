/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_NOOPCREDENTIALGENERATOR_HPP_
#define _GEMFIRE_NOOPCREDENTIALGENERATOR_HPP_

#include "CredentialGenerator.hpp"

namespace gemfire {
namespace testframework {
namespace security {

class NoopCredentialGenerator : public CredentialGenerator {
 public:
  NoopCredentialGenerator() : CredentialGenerator(ID_NOOP, "NOOP") {
    ;
    ;
  };

  std::string getInitArgs(std::string workingDir, bool userMode) {
    std::string additionalArgs;
    char* buildDir = ACE_OS::getenv("BUILDDIR");
    if (buildDir != NULL && workingDir.length() == 0) {
      workingDir = std::string(buildDir);
      workingDir += std::string("/framework/xml/Security/");
    }
    additionalArgs = " ";

    return additionalArgs;
  }

  std::string getClientAuthInitLoaderFactory() {
    return "createNoopAuthInitInstance";
  }
  std::string getClientAuthInitLoaderLibrary() { return "testobject"; }
  std::string getClientAuthenticator() {
    return "javaobject.NoopAuthenticator.create";
  }
  std::string getClientAuthorizer() { return "javaobject.NoopAccessor.create"; }
};

};  // security
};  // testframework
};  // gemfire

#endif
