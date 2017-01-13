/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    CredentialGenerator.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef _GEMFIRE_CREDENTIALGENERATOR_HPP_
#define _GEMFIRE_CREDENTIALGENERATOR_HPP_

// ----------------------------------------------------------------------------

#include <gfcpp/SharedBase.hpp>
#include <gfcpp/Properties.hpp>

#ifndef __COMPILE_DUNIT_
#include "fwklib/FwkLog.hpp"
#else
#ifndef FWKINFO
#define FWKINFO(x)
#endif
#ifndef FWKDEBUG
#define FWKDEBUG(x)
#endif
#endif

#include "typedefs.hpp"

#include <ace/OS.h>

#include <map>

namespace gemfire {
namespace testframework {
namespace security {

class CredentialGenerator;
typedef SharedPtr<CredentialGenerator> CredentialGeneratorPtr;

/**
 * @class CredentialGenerator CredentialGenerator.hpp
 * Encapsulates valid or invalid credentials required for Client connection
 * and server command parameters to be supplied.
 * <p>
 * Different implementations are there for different kinds of authentication/
 * authorization schemes.
 * <p>
 * Usage:
 *    CredentialGeneratorPtr cg = CredentialGenerator::create( schemeStr );
 *   @param schemeStr can be one of the following
 *    DUMMY - simple username/password authentication
 *    LDAP  - LDAP server based authentication.
 *    PKCS  - Digital signature based authentication
 *    NONE  - Disable security altogether.
 *
 * for client connection credentials
 *   cg->getAuthInit(prop);
 *   cg->getValidCredentials(prop);
 *
 * for server command line security arguments
 *   str = cg->getServerCmdParams(":authenticator:authorizer");
 *
 */
class CredentialGenerator : public SharedBase {
 public:
  typedef std::map<std::string, CredentialGeneratorPtr> registeredClassMap;

 private:
  ID m_id;
  std::string m_name;
  static registeredClassMap* generatormap;

  CredentialGenerator() : m_id(ID_NONE), m_name("NONE"){};

 protected:
  CredentialGenerator(ID id, std::string name) : m_id(id), m_name(name){};
  CredentialGenerator(const CredentialGenerator& other)
      : m_id(other.m_id), m_name(other.m_name) {}

 public:
  CredentialGenerator& operator=(const CredentialGenerator& other) {
    if (this == &other) return *this;

    this->m_id = other.m_id;
    this->m_name = other.m_name;

    return *this;
  }
  virtual ~CredentialGenerator() {}
  bool operator==(const CredentialGenerator* other) {
    if (other == NULL) return false;
    return (m_id == other->m_id && m_name == other->m_name);
  };

 public:
  static CredentialGeneratorPtr create(std::string scheme);
  static bool registerScheme(CredentialGeneratorPtr scheme) {
    // if not already registered...
    if (generators().find(scheme->m_name) == generators().end()) {
      generators()[scheme->m_name] = scheme;
      return true;
    }
    return false;
  }
  static registeredClassMap& getRegisteredSchemes() { return generators(); }

  static registeredClassMap& generators() {
    if (CredentialGenerator::generatormap == NULL) {
      CredentialGenerator::generatormap = new registeredClassMap;
    }
    return *CredentialGenerator::generatormap;
  }

  ID id() { return m_id; }
  std::string name() { return m_name; }

  std::string toString() {
    char chID[5];
    sprintf(chID, "%d", m_id);
    return std::string(chID) + m_name;
  }
  static void dump() {
    FWKINFO("dumping all registered classes ");
    registeredClassMap::iterator it = generators().begin();
    while (it != generators().end()) {
      FWKINFO(((*it).second)->toString());
      it++;
    }
  }

  void hashCode(){};

  void getAuthInit(PropertiesPtr& prop) {
    std::string authinit = this->getClientAuthInitLoaderFactory();
    if (!authinit.empty()) {
      FWKINFO("Authentication initializer : "
              << authinit << " library "
              << this->getClientAuthInitLoaderLibrary());
      prop->insert("security-client-auth-factory", authinit.c_str());
      prop->insert("security-client-auth-library",
                   this->getClientAuthInitLoaderLibrary().c_str());
    }
  }

  std::string getPublickeyfile() {
    char* tempPath = NULL;
    tempPath = ACE_OS::getenv("TESTSRC");
    std::string path = "";
    if (tempPath == NULL) {
      tempPath = ACE_OS::getenv("BUILDDIR");
      path = std::string(tempPath) + "/framework/data";
    } else {
      path = std::string(tempPath);
    }
    char pubfile[1000] = {'\0'};
    sprintf(pubfile, "%s/keystore/publickeyfile", path.c_str());
    return std::string(pubfile);
  }

  std::string getServerCmdParams(std::string securityParams,
                                 std::string workingDir = "",
                                 bool userMode = false) {
    std::string securityCmdStr;
    FWKINFO("User mode is " << userMode);
    if (securityParams.find("authenticator") != std::string::npos &&
        !this->getClientAuthenticator().empty()) {
      securityCmdStr = this->getInitArgs(workingDir, userMode);
      securityCmdStr +=
          std::string(" --J=-Dgemfire.security-client-authenticator=") +
          this->getClientAuthenticator();
    }
    if ((securityParams.find("authorizer") != std::string::npos) &&
        (!this->getClientAuthorizer().empty())) {
      securityCmdStr +=
          std::string(" --J=-Dgemfire.security-client-accessor=") +
          this->getClientAuthorizer();
    }
    if ((securityParams.find("authorizerPP") != std::string::npos) &&
        (!this->getClientAuthorizer().empty())) {
      securityCmdStr +=
          std::string(" --J=-Dgemfire.security-client-accessor-pp=") +
          this->getClientAuthorizer();
    }
    if (m_id == ID_PKI) {
      securityCmdStr +=
          std::string(" --J=-Dgemfire.security-publickey-filepath=") +
          this->getPublickeyfile();
      securityCmdStr +=
          std::string(" --J=-Dgemfire.security-publickey-pass=gemfire");
    }
    if ((securityParams.find("dummy") != std::string::npos) &&
        (!this->getClientDummyAuthorizer().empty())) {
      securityCmdStr +=
          std::string(" --J=-Dgemfire.security-client-accessor=") +
          this->getClientDummyAuthorizer();
    }
#ifdef __COMPILE_DUNIT_  // lets suppress -N option in case of unit tests.
    int idx;
    while ((idx = securityCmdStr.find("--J=-Dgemfire.", 0)) >= 0) {
      securityCmdStr.replace(idx, 2, "");
    }
#endif
    return securityCmdStr;
  }

  virtual void getValidCredentials(PropertiesPtr& p) {
    ;
    ;  // no credentials by default
  }
  virtual void getInvalidCredentials(PropertiesPtr& p) {
    ;
    ;  // no credentials by default
  }

  virtual void getAllowedCredentialsForOps(opCodeList& opCodes,
                                           PropertiesPtr& p,
                                           stringList* regionNames = NULL) {
    ;
    ;  // no credentials by default
  }
  virtual void getDisallowedCredentialsForOps(opCodeList& opCodes,
                                              PropertiesPtr& p,
                                              stringList* regionNames = NULL) {
    ;
    ;  // no credentials by default
  }

  static registeredClassMap& getRegisterdSchemes() {
    if (generators().size() == 0) {
      // calling for registering the existing schemes.
      create("");
    }
    return generators();
  }

 public:
  virtual std::string getClientAuthInitLoaderFactory() { return ""; }
  virtual std::string getClientAuthInitLoaderLibrary() { return ""; }
  virtual std::string getInitArgs(std::string workingDir, bool userMode) {
    return "";
  }
  virtual std::string getClientAuthenticator() { return ""; }
  virtual std::string getClientAuthorizer() { return ""; }
  virtual std::string getClientDummyAuthorizer() { return ""; }
};

};  // security
};  // testframework
};  // gemfire

#endif
