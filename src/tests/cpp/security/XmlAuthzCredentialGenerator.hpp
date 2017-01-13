/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_XMLAUTHZCREDENTIALGENERATOR_HPP_
#define _GEMFIRE_XMLAUTHZCREDENTIALGENERATOR_HPP_

#include <gfcpp/SharedBase.hpp>
#include <gfcpp/Properties.hpp>

#include "typedefs.hpp"

#include <algorithm>
#include <time.h>
#include <stdlib.h>

namespace gemfire {
namespace testframework {
namespace security {

const opCodeList::value_type RArr[] = {
    OP_GET,     OP_GETALL,      OP_REGISTER_INTEREST, OP_UNREGISTER_INTEREST,
    OP_KEY_SET, OP_CONTAINS_KEY};

const opCodeList::value_type WArr[] = {
    OP_CREATE,     OP_UPDATE,       OP_PUTALL,          OP_DESTROY,
    OP_INVALIDATE, OP_REGION_CLEAR, OP_EXECUTE_FUNCTION};

const opCodeList::value_type QArr[] = {OP_QUERY, OP_REGISTER_CQ};

const stringList::value_type QRArr[] = {"Portfolios", "Positions"};

const char* PRiUsnm = "%s%d";

class XmlAuthzCredentialGenerator;
typedef SharedPtr<XmlAuthzCredentialGenerator> XmlAuthzCredentialGeneratorPtr;

class XmlAuthzCredentialGenerator : public SharedBase {
 private:
  ID m_id;
  opCodeList* m_opCode;
  stringList* m_regionNames;
  PropertiesPtr* m_prop;

  opCodeList Readers;
  opCodeList Writers;
  opCodeList Query;
  stringList QueryRegions;

 public:
  XmlAuthzCredentialGenerator(ID id)
      : m_id(id),
        Readers(RArr, RArr + sizeof RArr / sizeof *RArr),
        Writers(WArr, WArr + sizeof WArr / sizeof *WArr),
        Query(QArr, QArr + sizeof QArr / sizeof *QArr),
        QueryRegions(QRArr, QRArr + sizeof QRArr / sizeof *QRArr) {
    m_opCode = NULL;
    m_regionNames = NULL;
    m_prop = NULL;
    /* initialize random seed: */
    srand((unsigned int)time(NULL));
  }
  virtual ~XmlAuthzCredentialGenerator() { ; }

  virtual void getAllowedCredentials(opCodeList& opCode, PropertiesPtr& prop,
                                     stringList* regionNames) {
    try {
      m_opCode = &opCode;
      m_regionNames = regionNames;
      m_prop = &prop;

      switch (m_id) {
        case ID_DUMMY:
          getAllowedDummyAuthz(NO_ROLE);
          break;
        case ID_LDAP:
          getAllowedLdapAuthz(NO_ROLE);
          break;
        case ID_PKI:
          getAllowedPkcsAuthz(NO_ROLE);
          break;
        default:
          break;
      };

    } catch (...) {
      reset();
    }

    reset();
  }

  void reset() {
    m_opCode = NULL;
    m_regionNames = NULL;
    m_prop = NULL;
  }

  virtual void getDisallowedCredentials(opCodeList& opCode, PropertiesPtr& prop,
                                        stringList* regionNames) {
    try {
      m_opCode = &opCode;
      m_regionNames = regionNames;
      m_prop = &prop;
      ROLES role = getRequiredRole();
      switch (role) {
        case READER_ROLE:
          role = WRITER_ROLE;
        case WRITER_ROLE:
          role = READER_ROLE;
        case QUERY_ROLE:
          role = WRITER_ROLE;
        case ADMIN_ROLE:
          role = QUERY_ROLE;
        default:
          /* UNNECESSARY role = role*/ break;
      };

      switch (m_id) {
        case ID_DUMMY:
          getAllowedDummyAuthz(role);
          break;
        case ID_LDAP:
          getAllowedLdapAuthz(role);
          break;
        case ID_PKI:
          getAllowedPkcsAuthz(role);
          break;
        default:
          break;
      };

    } catch (...) {
      reset();
    }

    reset();
  }

 private:
  void getAllowedDummyAuthz(ROLES role) {
    const char* adminUsers[] = {"admin", "root", "administrator"};
    const int adminUsrSz = (sizeof adminUsers / sizeof *adminUsers) - 1;
    std::string validity = "invalid";

    if (role == NO_ROLE) {
      role = getRequiredRole();
      validity = "valid";
    }
    char userName[100];
    switch (role) {
      case READER_ROLE:
        sprintf(userName, PRiUsnm, "reader", rand() % 3);
        break;
      case WRITER_ROLE:
        sprintf(userName, PRiUsnm, "writer", rand() % 3);
        break;
      case QUERY_ROLE:
        sprintf(userName, PRiUsnm, "reader", (rand() % 2) + 3);
        break;
      case ADMIN_ROLE:
        sprintf(userName, "%s", adminUsers[rand() % adminUsrSz]);
        break;
      default:
        sprintf(userName, PRiUsnm, "user", rand() % 3);
        break;
    };

    (*m_prop)->insert("security-username", userName);
    (*m_prop)->insert("security-password", userName);

    FWKINFO("inserted " << validity << " dummy security-username "
                        << (*m_prop)->find("security-username")->asChar()
                        << " password "
                        << ((*m_prop)->find("security-password") != NULLPTR
                                ? (*m_prop)->find("security-password")->asChar()
                                : "not set"));
  }

  std::string getAllowedUser(ROLES role) {
    const std::string userPrefix = "gemfire";
    const int readerIndices[] = {3, 4, 5};
    const int writerIndices[] = {6, 7, 8};
    const int queryIndices[] = {9, 10};
    const int adminIndices[] = {1, 2};
    const int readerIndSz = (sizeof readerIndices / sizeof *readerIndices) - 1;
    const int writerIndSz = (sizeof writerIndices / sizeof *writerIndices) - 1;
    const int queryIndSz = (sizeof queryIndices / sizeof *queryIndices) - 1;
    const int adminIndSz = (sizeof adminIndices / sizeof *adminIndices) - 1;

    std::string validity = "invalid";

    if (role == NO_ROLE) {
      role = getRequiredRole();
      validity = "valid";
    }
    char userName[256];
    switch (role) {
      case READER_ROLE:
        sprintf(userName, PRiUsnm, userPrefix.c_str(),
                readerIndices[rand() % readerIndSz]);
        break;
      case WRITER_ROLE:
        sprintf(userName, PRiUsnm, userPrefix.c_str(),
                writerIndices[rand() % writerIndSz]);
        break;
      case QUERY_ROLE:
        sprintf(userName, PRiUsnm, userPrefix.c_str(),
                queryIndices[rand() % queryIndSz]);
        break;
      case ADMIN_ROLE:
      default:
        sprintf(userName, PRiUsnm, userPrefix.c_str(),
                adminIndices[rand() % adminIndSz]);
        break;
    };
    FWKINFO("inserted " << validity << " username " << userName);
    return std::string(userName);
  }

  void getAllowedLdapAuthz(ROLES role) {
    const std::string userName = getAllowedUser(role);
    (*m_prop)->insert("security-username", userName.c_str());
    (*m_prop)->insert("security-password", userName.c_str());

    FWKINFO("inserted  ldap security-username "
            << (*m_prop)->find("security-username")->asChar() << " password "
            << ((*m_prop)->find("security-password") != NULLPTR
                    ? (*m_prop)->find("security-password")->asChar()
                    : "not set"));
  }

  void getAllowedPkcsAuthz(ROLES role) {
    const std::string userName = getAllowedUser(role);
    (*m_prop)->insert("security-alias", userName.c_str());
    (*m_prop)->insert("security-keystorepass", "gemfire");

    FWKINFO("inserted  PKCS security-alias"
            << (*m_prop)->find("security-alias")->asChar() << " password "
            << ((*m_prop)->find("security-keystorepass") != NULLPTR
                    ? (*m_prop)->find("security-keystorepass")->asChar()
                    : "not set"));
  }

  ROLES getRequiredRole() {
    bool requireReaders = true, requireWriters = true, requireQuery = true;
    ROLES role = ADMIN_ROLE;

    for (opCodeList::iterator it = m_opCode->begin(); it != m_opCode->end();
         it++) {
      if (requireReaders &&
          std::find(Readers.begin(), Readers.end(), (*it)) == Readers.end()) {
        requireReaders = false;
      }

      if (requireWriters &&
          std::find(Writers.begin(), Writers.end(), (*it)) == Writers.end()) {
        requireWriters = false;
      }

      if (requireQuery &&
          std::find(Query.begin(), Query.end(), (*it)) == Query.end()) {
        requireQuery = false;
      }
    }

    if (requireReaders) {
      role = READER_ROLE;
    } else if (requireWriters) {
      role = WRITER_ROLE;
    } else if (requireQuery) {
      role = QUERY_ROLE;
      /*
       if( m_regionNames != NULL && m_regionNames->size() > 0 ) {
         bool queryUsers = true;
         for( stringList::iterator rit = m_regionNames->begin(); rit !=
       m_regionNames->end(); rit++) {
            if( queryUsers && std::find(QueryRegions.begin(),
       QueryRegions.end(),(*rit)) == QueryRegions.end() ) {
              queryUsers = false;
            }
         }
         if( queryUsers ) {
           role = QUERY_ROLE;
         }
       }
       */
    }

    return role;
  }  // end of requireRole
};

};  // security
};  // testframework
};  // gemfire

#endif /*_GEMFIRE_AUTHZCREDENTIALGENERATOR_HPP_*/
