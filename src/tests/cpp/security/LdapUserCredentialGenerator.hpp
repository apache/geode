#pragma once

#ifndef APACHE_GEODE_GUARD_13e33c8479a332a850fc4a6ded808476
#define APACHE_GEODE_GUARD_13e33c8479a332a850fc4a6ded808476

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

#include <ace/ACE.h>
#include <ace/OS.h>

namespace apache {
namespace geode {
namespace client {
namespace testframework {
namespace security {

class LdapUserCredentialGenerator : public CredentialGenerator {
 public:
  LdapUserCredentialGenerator() : CredentialGenerator(ID_LDAP, "LDAP") {
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

    additionalArgs = std::string(" --J=-Dgemfire.security-authz-xml-uri=") +
                     std::string(workingDir) + std::string("authz-ldap.xml");

    char* ldapSrv = ACE_OS::getenv("LDAP_SERVER");
    additionalArgs += std::string(" --J=-Dgemfire.security-ldap-server=") +
                      (ldapSrv != NULL ? ldapSrv : "ldap");

    char* ldapRoot = ACE_OS::getenv("LDAP_BASEDN");
    additionalArgs +=
        std::string(" --J=\\\"-Dgemfire.security-ldap-basedn=") +
        (ldapRoot != NULL ? ldapRoot
                          : "ou=ldapTesting,dc=ldap,dc=gemstone,dc=com") +
        "\\\"";

    char* ldapSSL = ACE_OS::getenv("LDAP_USESSL");
    additionalArgs += std::string(" --J=-Dgemfire.security-ldap-usessl=") +
                      (ldapSSL != NULL ? ldapSSL : "false");

    return additionalArgs;
  }

  std::string getClientAuthInitLoaderFactory() {
    return "createUserPasswordAuthInitInstance";
  }
  std::string getClientAuthInitLoaderLibrary() { return "securityImpl"; }
  std::string getClientAuthenticator() {
    return "javaobject.LdapUserAuthenticator.create";
  }
  std::string getClientAuthorizer() {
    return "javaobject.XmlAuthorization.create";
  }

  std::string getClientDummyAuthorizer() {
    return "javaobject.DummyAuthorization.create";
  }
  void getValidCredentials(PropertiesPtr& p) {
    p->insert("security-username", "gemfire1");
    p->insert("security-password", "gemfire1");
    FWKDEBUG("inserted valid security-username "
             << p->find("security-username")->asChar() << " password "
             << p->find("security-password")->asChar());
  }

  void getInvalidCredentials(PropertiesPtr& p) {
    p->insert("security-username", "gemfire1");
    p->insert("security-password", "1gemfire");
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


#endif // APACHE_GEODE_GUARD_13e33c8479a332a850fc4a6ded808476
