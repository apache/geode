#pragma once

#ifndef APACHE_GEODE_GUARD_329d127281ca21e68d7ebee13db94333
#define APACHE_GEODE_GUARD_329d127281ca21e68d7ebee13db94333

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

#include "CredentialGenerator.hpp"
#include "XmlAuthzCredentialGenerator.hpp"

namespace apache {
namespace geode {
namespace client {
namespace testframework {
namespace security {

class DummyCredentialGenerator : public CredentialGenerator {
 public:
  DummyCredentialGenerator() : CredentialGenerator(ID_DUMMY, "DUMMY") {
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
    FWKINFO("Inside dummy Credentials usermode is " << userMode);
    if (userMode) {
      additionalArgs = std::string(" --J=-Dgemfire.security-authz-xml-uri=") +
                       std::string(workingDir) +
                       std::string("authz-dummyMU.xml");
    } else {
      additionalArgs = std::string(" --J=-Dgemfire.security-authz-xml-uri=") +
                       std::string(workingDir) + std::string("authz-dummy.xml");
    }

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
    return "javaobject.XmlAuthorization.create";
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

}  // namespace security
}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_329d127281ca21e68d7ebee13db94333
