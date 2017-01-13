/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_DUMMYCREDENTIALGENERATOR2_HPP_
#define _GEMFIRE_DUMMYCREDENTIALGENERATOR2_HPP_

#include "CredentialGenerator.hpp"
#include "XmlAuthzCredentialGenerator.hpp"

namespace gemfire {
namespace testframework {
namespace security {

class DummyCredentialGenerator2 : public CredentialGenerator {
 public:
  DummyCredentialGenerator2() : CredentialGenerator(ID_DUMMY2, "DUMMY2") {
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
    if (userMode)
      additionalArgs = std::string(" --J=-Dgemfire.security-authz-xml-uri=") +
                       std::string(workingDir) +
                       std::string("authz-dummyMU.xml");
    else
      additionalArgs = std::string(" --J=-Dgemfire.security-authz-xml-uri=") +
                       std::string(workingDir) + std::string("authz-dummy.xml");

    return additionalArgs;
  }

  std::string getClientAuthInitLoaderFactory() {
    return "createUserPasswordAuthInitInstance";
  }
  std::string getClientAuthInitLoaderLibrary() { return "securityImpl"; }
  std::string getClientAuthenticator() {
    return "javaobject.DummyAuthenticator.create";
  }
  std::string getClientAuthorizer() {
    return "javaobject.DummyAuthorization2.create";
  }
  std::string getClientDummyAuthorizer() {
    return "javaobject.DummyAuthorization.create";
  }

  void getValidCredentials(PropertiesPtr& p) {
    p->insert("security-username", "user1");
    p->insert("security-password", "user1");
    FWKDEBUG("inserted valid security-username "
             << p->find("security-username")->asChar() << " password "
             << p->find("security-password")->asChar());
  }

  void getInvalidCredentials(PropertiesPtr& p) {
    p->insert("security-username", "1user");
    p->insert("security-password", "user1");
    FWKDEBUG("inserted invalid security-username "
             << p->find("security-username")->asChar() << " password "
             << p->find("security-password")->asChar());
  }

  void getAllowedCredentialsForOps(opCodeList& opCodes, PropertiesPtr& p,
                                   stringList* regionNames = NULL) {
    XmlAuthzCredentialGenerator authz(id());
    authz.getAllowedCredentials(opCodes, p, regionNames);
  }

  void getDisallowedCredentialsForOps(opCodeList& opCodes, PropertiesPtr& p,
                                      stringList* regionNames = NULL) {
    XmlAuthzCredentialGenerator authz(id());
    authz.getDisallowedCredentials(opCodes, p, regionNames);
  }
};

};  // security
};  // testframework
};  // gemfire

#endif
