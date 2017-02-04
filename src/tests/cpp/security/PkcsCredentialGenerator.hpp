#pragma once

#ifndef APACHE_GEODE_GUARD_7531b050ea22f83a394ce20e9ea4949d
#define APACHE_GEODE_GUARD_7531b050ea22f83a394ce20e9ea4949d

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

const char SECURITY_USERNAME[] = "security-username";
const char KEYSTORE_FILE_PATH[] = "security-keystorepath";
const char KEYSTORE_ALIAS[] = "security-alias";
const char KEYSTORE_PASSWORD[] = "security-keystorepass";

#include <ace/ACE.h>
#include <ace/OS.h>

namespace apache {
namespace geode {
namespace client {
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

    sprintf(keyStorePassWord, "%s", "geode");
    p->insert(SECURITY_USERNAME, "geode");
    p->insert(KEYSTORE_ALIAS, username);
    p->insert(KEYSTORE_PASSWORD, keyStorePassWord);
    insertKeyStorePath(p, username);
  }

  void getValidCredentials(PropertiesPtr& p) {
    char username[20] = {'\0'};
    sprintf(username, "geode%d", (rand() % 10) + 1);
    setPKCSProperties(p, username);
    FWKINFO("inserted valid security-username "
            << p->find("security-username")->asChar());
  }

  void getInvalidCredentials(PropertiesPtr& p) {
    char username[20] = {'\0'};
    sprintf(username, "%dgeode", (rand() % 11) + 1);
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

}  // namespace security
}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_7531b050ea22f83a394ce20e9ea4949d
