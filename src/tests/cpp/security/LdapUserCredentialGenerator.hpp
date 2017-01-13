/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_LDAPUSERCREDENTIALGENERATOR_HPP_
#define _GEMFIRE_LDAPUSERCREDENTIALGENERATOR_HPP_

#include "CredentialGenerator.hpp"
#include "XmlAuthzCredentialGenerator.hpp"

#include <ace/ACE.h>
#include <ace/OS.h>

namespace gemfire {
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

};  // security
};  // testframework
};  // gemfire

#endif /*_GEMFIRE_LDAPUSERCREDENTIALGENERATOR_HPP_*/
