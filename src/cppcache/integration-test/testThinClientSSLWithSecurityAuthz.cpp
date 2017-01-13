/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/FunctionService.hpp>
#include <gfcpp/Execution.hpp>

#define ROOT_NAME "testThinClientSSLWithSecurityAuthz"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"
#include "ThinClientHelper.hpp"
#include "ace/Process.h"

#include "ThinClientSecurity.hpp"

using namespace gemfire::testframework::security;
using namespace gemfire;

const char* locHostPort =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
CredentialGeneratorPtr credentialGeneratorHandler;

std::string getXmlPath() {
  char xmlPath[1000] = {'\0'};
  const char* path = ACE_OS::getenv("TESTSRC");
  ASSERT(path != NULL,
         "Environment variable TESTSRC for test source directory is not set.");
  strncpy(xmlPath, path, strlen(path) - strlen("cppcache"));
  strcat(xmlPath, "xml/Security/");
  return std::string(xmlPath);
}

void initCredentialGenerator() {
  static int loopNum = 1;

  switch (loopNum) {
    case 1: {
      credentialGeneratorHandler = CredentialGenerator::create("DUMMY");
      break;
    }
    case 2: {
      credentialGeneratorHandler = CredentialGenerator::create("LDAP");
      break;
    }
    default:
    case 3: {
      credentialGeneratorHandler = CredentialGenerator::create("PKCS");
      break;
    }
  }

  if (credentialGeneratorHandler == NULLPTR) {
    FAIL("credentialGeneratorHandler is NULL");
  }

  loopNum++;
  if (loopNum > 3) loopNum = 1;
}

opCodeList::value_type tmpRArr[] = {
    OP_GET,     OP_GETALL,      OP_REGISTER_INTEREST, OP_UNREGISTER_INTEREST,
    OP_KEY_SET, OP_CONTAINS_KEY};

opCodeList::value_type tmpWArr[] = {OP_CREATE,  OP_UPDATE,     OP_PUTALL,
                                    OP_DESTROY, OP_INVALIDATE, OP_REGION_CLEAR};

opCodeList::value_type tmpAArr[] = {OP_CREATE,       OP_UPDATE,
                                    OP_DESTROY,      OP_INVALIDATE,
                                    OP_REGION_CLEAR, OP_REGISTER_INTEREST,
                                    OP_GET,          OP_QUERY,
                                    OP_REGISTER_CQ,  OP_EXECUTE_FUNCTION};

#define HANDLE_NO_NOT_AUTHORIZED_EXCEPTION                 \
  catch (const gemfire::NotAuthorizedException&) {         \
    LOG("NotAuthorizedException Caught");                  \
    FAIL("should not have caught NotAuthorizedException"); \
  }                                                        \
  catch (const gemfire::Exception& other) {                \
    LOG("Got gemfire::Exception& other ");                 \
    other.printStackTrace();                               \
    FAIL(other.getMessage());                              \
  }

#define HANDLE_NOT_AUTHORIZED_EXCEPTION            \
  catch (const gemfire::NotAuthorizedException&) { \
    LOG("NotAuthorizedException Caught");          \
    LOG("Success");                                \
  }                                                \
  catch (const gemfire::Exception& other) {        \
    other.printStackTrace();                       \
    FAIL(other.getMessage());                      \
  }

#define ADMIN_CLIENT s1p1
#define WRITER_CLIENT s1p2
#define READER_CLIENT s2p1
//#define USER_CLIENT s2p2

#define TYPE_ADMIN_CLIENT 'A'
#define TYPE_WRITER_CLIENT 'W'
#define TYPE_READER_CLIENT 'R'
#define TYPE_USER_CLIENT 'U'

const char* regionNamesAuth[] = {"DistRegionAck"};

void initClientAuth(char UserType) {
  PropertiesPtr config = Properties::create();
  opCodeList wr(tmpWArr, tmpWArr + sizeof tmpWArr / sizeof *tmpWArr);
  opCodeList rt(tmpRArr, tmpRArr + sizeof tmpRArr / sizeof *tmpRArr);
  opCodeList ad(tmpAArr, tmpAArr + sizeof tmpAArr / sizeof *tmpAArr);
  credentialGeneratorHandler->getAuthInit(config);
  switch (UserType) {
    case 'W':
      credentialGeneratorHandler->getAllowedCredentialsForOps(wr, config, NULL);
      break;
    case 'R':
      credentialGeneratorHandler->getAllowedCredentialsForOps(rt, config, NULL);
      break;
    case 'A':
      credentialGeneratorHandler->getAllowedCredentialsForOps(ad, config, NULL);
    default:
      break;
  }

  CacheableStringPtr alias(config->find("security-alias"));
  CacheableStringPtr uname(config->find("security-username"));
  CacheableStringPtr passwd(config->find("security-password"));

  char msgAlias[100];
  char msgUname[100];
  char msgPasswd[100];

  sprintf(msgAlias, "PKCS alias is %s",
          alias == NULLPTR ? "null" : alias->asChar());
  sprintf(msgUname, "username is %s",
          uname == NULLPTR ? "null" : uname->asChar());
  sprintf(msgPasswd, "password is %s",
          passwd == NULLPTR ? "null" : passwd->asChar());

  LOG(msgAlias);
  LOG(msgUname);
  LOG(msgPasswd);

  config->insert("ssl-enabled", "true");
  std::string keystore = std::string(ACE_OS::getenv("TESTSRC")) + "/keystore";
  std::string pubkey = keystore + "/client_truststore.pem";
  std::string privkey = keystore + "/client_keystore.pem";
  config->insert("ssl-keystore", privkey.c_str());
  config->insert("ssl-truststore", pubkey.c_str());

  try {
    initClient(true, config);
  } catch (...) {
    throw;
  }
}

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StartServer1_With_SSL)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          1, "cacheserver_notify_subscription.xml", locHostPort,
          const_cast<char*>(cmdServerAuthenticator.c_str()), true);
      LOG("Server1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StartServer2_With_SSL)
  {
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          2, "cacheserver_notify_subscription2.xml", locHostPort,
          const_cast<char*>(cmdServerAuthenticator.c_str()), true);
      LOG("Server2 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StartLocator_With_SSL)
  {
    if (isLocator) {
      CacheHelper::initLocator(1, true);
      LOG("Locator1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StepOne)
  {
    initClientAuth('A');
    try {
      LOG("Tying Region creation");
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, true);
      LOG("Region created successfully");
      LOG("Tying Entry creation");
      createEntry(regionNamesAuth[0], keys[0], vals[0]);
      LOG("Entry created successfully");
      updateEntry(regionNamesAuth[0], keys[0], nvals[0]);
      LOG("Entry updated successfully");
      HashMapOfCacheable entrymap;
      entrymap.clear();
      for (int i = 0; i < 5; i++) {
        entrymap.insert(CacheableKey::create(i), CacheableInt32::create(i));
      }
      RegionPtr regPtr = getHelper()->getRegion(regionNamesAuth[0]);
      regPtr->putAll(entrymap);
      LOG("PutAll completed successfully");
      for (int i = 0; i < 5; i++) {
        regPtr->invalidate(CacheableKey::create(i));
      }
      VectorOfCacheableKey entrykeys;
      for (int i = 0; i < 5; i++) {
        entrykeys.push_back(CacheableKey::create(i));
      }
      HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
      valuesMap->clear();
      regPtr->getAll(entrykeys, valuesMap, NULLPTR, false);
      if (valuesMap->size() > 0) {
        LOG("GetAll completed successfully");
      } else {
        FAIL("GetAll did not complete successfully");
      }
      regPtr->query("1=1");
      LOG("Query completed successfully");
      PoolPtr pool = PoolManager::find(regionNamesAuth[0]);
      QueryServicePtr qs;
      if (pool != NULLPTR) {
        // Using region name as pool name
        qs = pool->getQueryService();
      } else {
        qs = getHelper()->cachePtr->getQueryService();
      }
      char queryString[100];
      sprintf(queryString, "select * from /%s", regionNamesAuth[0]);
      CqAttributesFactory cqFac;
      CqAttributesPtr cqAttrs(cqFac.create());
      CqQueryPtr qry = qs->newCq("cq_security", queryString, cqAttrs);
      qs->executeCqs();
      LOG("CQ completed successfully");
      if (pool != NULLPTR) {
        FunctionService::onServer(pool)
            ->execute("securityTest", true)
            ->getResult();
        LOG("Function execution completed successfully");
      } else {
        LOG("Skipping function execution for non pool case");
      }
      invalidateEntry(regionNamesAuth[0], keys[0]);
      LOG("Entry invalidated successfully");
      verifyInvalid(regionNamesAuth[0], keys[0]);
      LOG("Entry invalidate-verified successfully");
      destroyEntry(regionNamesAuth[0], keys[0]);
      LOG("Entry destroyed successfully");
      verifyDestroyed(regionNamesAuth[0], keys[0]);
      LOG("Entry destroy-verified successfully");
      destroyRegion(regionNamesAuth[0]);
      LOG("Region destroy successfully");
      LOG("Tying Region creation");
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, true);
      LOG("Region created successfully");
      createEntry(regionNamesAuth[0], keys[2], vals[2]);
      LOG("Entry created successfully");
      RegionPtr regPtr0 = getHelper()->getRegion(regionNamesAuth[0]);
      if (regPtr0 != NULLPTR) {
        LOG("Going to do registerAllKeys");
        regPtr0->registerAllKeys();
        LOG("Going to do unregisterAllKeys");
        regPtr0->unregisterAllKeys();
      }
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(WRITER_CLIENT, StepTwo)
  {
    initCredentialGenerator();
    initClientAuth('W');
    try {
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, true);
      LOG("Region created successfully");
      createEntry(regionNamesAuth[0], keys[0], vals[0]);
      LOG("Entry created successfully");
      updateEntry(regionNamesAuth[0], keys[0], nvals[0]);
      LOG("Entry updated successfully");
      HashMapOfCacheable entrymap;
      entrymap.clear();
      for (int i = 0; i < 5; i++) {
        entrymap.insert(CacheableKey::create(i), CacheableInt32::create(i));
      }
      RegionPtr regPtr = getHelper()->getRegion(regionNamesAuth[0]);
      regPtr->putAll(entrymap);
      LOG("PutAll completed successfully");
      invalidateEntry(regionNamesAuth[0], keys[0]);
      LOG("Entry invalidated successfully");
      verifyInvalid(regionNamesAuth[0], keys[0]);
      LOG("Entry invalidate-verified successfully");
      destroyEntry(regionNamesAuth[0], keys[0]);
      LOG("Entry destroyed successfully");
      verifyDestroyed(regionNamesAuth[0], keys[0]);
      LOG("Entry destroy-verified successfully");
      createEntry(regionNamesAuth[0], keys[0], vals[0]);
      LOG("Entry created successfully");
      updateEntry(regionNamesAuth[0], keys[0], nvals[0]);
      LOG("Entry updated successfully");
      verifyEntry(regionNamesAuth[0], keys[0], nvals[0]);
      LOG("Entry updation-verified successfully");
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION
    try {
      RegionPtr regPtr0 = getHelper()->getRegion(regionNamesAuth[0]);
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[2]);
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(regPtr0->get(keyPtr));
      if (checkPtr != NULLPTR) {
        char buf[1024];
        sprintf(buf, "In net search, get returned %s for key %s",
                checkPtr->asChar(), keys[2]);
        LOG(buf);
        FAIL("Should not get the value");
      } else {
        LOG("checkPtr is NULL");
      }
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesAuth[0]);
    try {
      LOG("Going to do registerAllKeys");
      regPtr0->registerAllKeys();
      FAIL("Should not be able to do Register Interest");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      for (int i = 0; i < 5; i++) {
        regPtr0->invalidate(CacheableKey::create(i));
      }
      VectorOfCacheableKey entrykeys;
      for (int i = 0; i < 5; i++) {
        entrykeys.push_back(CacheableKey::create(i));
      }
      HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
      valuesMap->clear();
      regPtr0->getAll(entrykeys, valuesMap, NULLPTR, false);
      if (valuesMap->size() > 0) {
        FAIL("GetAll should not have completed successfully");
      }
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      regPtr0->query("1=1");
      FAIL("Query should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    PoolPtr pool = PoolManager::find(regionNamesAuth[0]);

    try {
      QueryServicePtr qs;
      if (pool != NULLPTR) {
        // Using region name as pool name
        qs = pool->getQueryService();
      } else {
        qs = getHelper()->cachePtr->getQueryService();
      }
      char queryString[100];
      sprintf(queryString, "select * from /%s", regionNamesAuth[0]);
      CqAttributesFactory cqFac;
      CqAttributesPtr cqAttrs(cqFac.create());
      CqQueryPtr qry = qs->newCq("cq_security", queryString, cqAttrs);
      qs->executeCqs();
      FAIL("CQ should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      if (pool != NULLPTR) {
        FunctionService::onServer(pool)
            ->execute("securityTest", true)
            ->getResult();
        //   FAIL("Function execution should not have completed successfully");
      } else {
        LOG("Skipping function execution for non pool case");
      }
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    createEntry(regionNamesAuth[0], keys[2], vals[2]);
    LOG("Entry created successfully");

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(READER_CLIENT, StepThree)
  {
    initCredentialGenerator();
    initClientAuth('R');
    RegionPtr rptr;
    char buf[100];
    int i = 102;

    createRegionForSecurity(regionNamesAuth[0], USE_ACK, true);

    rptr = getHelper()->getRegion(regionNamesAuth[0]);
    sprintf(buf, "%s: %d", rptr->getName(), i);
    CacheableKeyPtr key = createKey(buf);
    sprintf(buf, "testUpdate::%s: value of %d", rptr->getName(), i);
    CacheableStringPtr valuePtr = CacheableString::create(buf);
    try {
      LOG("Trying put Operation");
      rptr->put(key, valuePtr);
      LOG(" Put Operation Successful");
      FAIL("Should have got NotAuthorizedException during put");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      LOG("Trying createEntry");
      createEntry(regionNamesAuth[0], keys[2], vals[2]);
      FAIL("Should have got NotAuthorizedException during createEntry");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    ASSERT(!rptr->containsKey(keys[2]),
           "Key should not have been found in the region");

    try {
      LOG("Trying updateEntry");
      updateEntry(regionNamesAuth[0], keys[2], nvals[2], false, false);
      FAIL("Should have got NotAuthorizedException during updateEntry");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    ASSERT(!rptr->containsKey(keys[2]),
           "Key should not have been found in the region");

    try {
      RegionPtr regPtr0 = getHelper()->getRegion(regionNamesAuth[0]);
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[2]);
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(regPtr0->get(keyPtr));
      if (checkPtr != NULLPTR) {
        char buf[1024];
        sprintf(buf, "In net search, get returned %s for key %s",
                checkPtr->asChar(), keys[2]);
        LOG(buf);
      } else {
        LOG("checkPtr is NULL");
      }
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION

    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesAuth[0]);
    if (regPtr0 != NULLPTR) {
      try {
        LOG("Going to do registerAllKeys");
        regPtr0->registerAllKeys();
        LOG("Going to do unregisterAllKeys");
        regPtr0->unregisterAllKeys();
      }
      HANDLE_NO_NOT_AUTHORIZED_EXCEPTION
    }

    try {
      HashMapOfCacheable entrymap;
      entrymap.clear();
      for (int i = 0; i < 5; i++) {
        entrymap.insert(CacheableKey::create(i), CacheableInt32::create(i));
      }
      regPtr0->putAll(entrymap);
      FAIL("PutAll should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      VectorOfCacheableKey entrykeys;
      for (int i = 0; i < 5; i++) {
        entrykeys.push_back(CacheableKey::create(i));
      }
      HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
      valuesMap->clear();
      regPtr0->getAll(entrykeys, valuesMap, NULLPTR, false);
      if (valuesMap->size() > 0) {
        LOG("GetAll completed successfully");
      } else {
        FAIL("GetAll did not complete successfully");
      }
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION

    try {
      regPtr0->query("1=1");
      FAIL("Query should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    PoolPtr pool = PoolManager::find(regionNamesAuth[0]);

    try {
      QueryServicePtr qs;
      if (pool != NULLPTR) {
        // Using region name as pool name
        qs = pool->getQueryService();
      } else {
        qs = getHelper()->cachePtr->getQueryService();
      }
      char queryString[100];
      sprintf(queryString, "select * from /%s", regionNamesAuth[0]);
      CqAttributesFactory cqFac;
      CqAttributesPtr cqAttrs(cqFac.create());
      CqQueryPtr qry = qs->newCq("cq_security", queryString, cqAttrs);
      qs->executeCqs();
      //    FAIL("CQ should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      if (pool != NULLPTR) {
        FunctionService::onServer(pool)
            ->execute("securityTest", true)
            ->getResult();
        FAIL("Function execution should not have completed successfully");
      } else {
        LOG("Skipping function execution for non pool case");
      }
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, CloseServer1)
  {
    SLEEP(9000);
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, CloseLocator_With_SSL)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1, true);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, CloseCacheAdmin)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(WRITER_CLIENT, CloseCacheWriter)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(READER_CLIENT, CloseCacheReader)
  { cleanProc(); }
END_TASK_DEFINITION

void doThinClientSSLWithSecurityAuthorization() {
  CALL_TASK(StartLocator_With_SSL);
  CALL_TASK(StartServer1_With_SSL);

  CALL_TASK(StepOne);
  CALL_TASK(StepTwo);
  CALL_TASK(StartServer2_With_SSL);
  CALL_TASK(CloseServer1);
  CALL_TASK(StepThree);
  CALL_TASK(CloseCacheReader);
  CALL_TASK(CloseCacheWriter);
  CALL_TASK(CloseCacheAdmin);
  CALL_TASK(CloseServer2);

  CALL_TASK(CloseLocator_With_SSL);
}

DUNIT_MAIN
  { doThinClientSSLWithSecurityAuthorization(); }
END_MAIN
