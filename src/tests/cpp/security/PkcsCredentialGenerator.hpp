/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_PKCSCREDENTIALGENERATOR_HPP_
#define _GEMFIRE_PKCSCREDENTIALGENERATOR_HPP_

#include "CredentialGenerator.hpp"
#include "XmlAuthzCredentialGenerator.hpp"

const char SECURITY_USERNAME[] = "security-username";
const char KEYSTORE_FILE_PATH[] = "security-keystorepath";
const char KEYSTORE_ALIAS[] = "security-alias";
const char KEYSTORE_PASSWORD[] = "security-keystorepass";

#include <ace/ACE.h>
#include <ace/OS.h>

namespace gemfire {
namespace testframework {
namespace security {

class PKCSCredentialGenerator : public CredentialGenerator {
 public:
  PKCSCredentialGenerator() : CredentialGenerator(ID_PKI, "PKCS"){};

  std::string getInitArgs(std::string workingDir, bool userMode) {
    FWKINFO("Inside PKCS credentials");
    std::string additionalArgs;
    char* buildDir = ACE_OS::getenv("BUILDDIR");

    if (buildDir != NULL && workingDir.length() == 0) {
      workingDir = std::string(buildDir);
      workingDir += std::string("/framework/xml/Security/");
    }

    if (buildDir != NULL && workingDir.length() == 0) {
      workingDir = std::string(buildDir);
      workingDir += std::string("/framework/xml/Security/");
    }

    char* authzXmlUri = ACE_OS::getenv("AUTHZ_XML_URI");
    additionalArgs =
        std::string(" --J=-Dgemfire.security-authz-xml-uri=") +
        std::string(workingDir) +
        std::string(authzXmlUri != NULL ? authzXmlUri : "authz-pkcs.xml");

    return additionalArgs;
  }

  std::string getClientAuthInitLoaderFactory() {
    return "createPKCSAuthInitInstance";
  }
  std::string getClientAuthInitLoaderLibrary() { return "securityImpl"; }
  std::string getClientAuthenticator() {
    return "javaobject.PKCSAuthenticator.create";
  }
  std::string getClientAuthorizer() {
    return "javaobject.XmlAuthorization.create";
  }
  std::string getClientDummyAuthorizer() {
    return "javaobject.DummyAuthorization.create";
  }

  void insertKeyStorePath(PropertiesPtr& p, const char* username) {
    char keystoreFilePath[1024];
    char* tempPath = NULL;
    tempPath = ACE_OS::getenv("TESTSRC");
    std::string path = "";
    if (tempPath == NULL) {
      tempPath = ACE_OS::getenv("BUILDDIR");
      path = std::string(tempPath) + "/framework/data";
    } else {
      path = std::string(tempPath);
    }

    sprintf(keystoreFilePath, "%s/keystore/%s.keystore", path.c_str(),
            username);
    p->insert(KEYSTORE_FILE_PATH, keystoreFilePath);
  }

  void setPKCSProperties(PropertiesPtr& p, char* username) {
    char keyStorePassWord[1024];

    sprintf(keyStorePassWord, "%s", "gemfire");
    p->insert(SECURITY_USERNAME, "gemfire");
    p->insert(KEYSTORE_ALIAS, username);
    p->insert(KEYSTORE_PASSWORD, keyStorePassWord);
    insertKeyStorePath(p, username);
  }

  void getValidCredentials(PropertiesPtr& p) {
    char username[20] = {'\0'};
    sprintf(username, "gemfire%d", (rand() % 10) + 1);
    setPKCSProperties(p, username);
    FWKINFO("inserted valid security-username "
            << p->find("security-username")->asChar());
  }

  void getInvalidCredentials(PropertiesPtr& p) {
    char username[20] = {'\0'};
    sprintf(username, "%dgemfire", (rand() % 11) + 1);
    setPKCSProperties(p, username);
    FWKINFO("inserted invalid security-username "
            << p->find("security-username")->asChar());
  }

  void getAllowedCredentialsForOps(opCodeList& opCodes, PropertiesPtr& p,
                                   stringList* regionNames = NULL) {
    XmlAuthzCredentialGenerator authz(id());
    authz.getAllowedCredentials(opCodes, p, regionNames);
    const char* username = p->find("security-alias")->asChar();
    insertKeyStorePath(p, username);
  }

  void getDisallowedCredentialsForOps(opCodeList& opCodes, PropertiesPtr& p,
                                      stringList* regionNames = NULL) {
    XmlAuthzCredentialGenerator authz(id());
    authz.getDisallowedCredentials(opCodes, p, regionNames);
    const char* username = p->find("security-alias")->asChar();
    insertKeyStorePath(p, username);
  }
};

};  // security
};  // testframework
};  // gemfire

#endif /*_GEMFIRE_PKCSCREDENTIALGENERATOR_HPP_*/
