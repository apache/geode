/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientSecurityAuthenticationMU"

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include "ThinClientSecurity.hpp"

using namespace gemfire;
using namespace test;

#define CORRECT_CREDENTIALS 'C'
#define INCORRECT_CREDENTIALS 'I'
#define NOT_PROVIDED_CREDENTIALS 'N'

const char* locHostPort =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* regionNamesAuth[] = {"DistRegionAck", "DistRegionNoAck"};
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
  if (loopNum > 2) loopNum = 1;
}
PropertiesPtr userCreds;
void initClientAuth(char credentialsType) {
  printf(" in initclientAuth 0 = %c ", credentialsType);
  userCreds = Properties::create();
  PropertiesPtr config = Properties::create();
  if (credentialGeneratorHandler == NULLPTR) {
    FAIL("credentialGeneratorHandler is NULL");
  }
  bool insertAuthInit = true;
  switch (credentialsType) {
    case 'C':
      LOG(" in initclientAuth0.00");
      credentialGeneratorHandler->getValidCredentials(userCreds);
      // config->insert("security-password" ,
      // config->find("security-username")->asChar() );
      // printf("Username is %s and Password is %s
      // ",userCreds->find("security-username")->asChar(),userCreds->find("security-password")->asChar());
      break;
    case 'I':
      LOG(" in initclientAuth0.0");
      credentialGeneratorHandler->getInvalidCredentials(userCreds);
      // config->insert("security-password" , "junk");
      //   printf("Username is %s and Password is %s
      //   ",userCreds->find("security-username")->asChar(),userCreds->find("security-password")->asChar());
      break;
    case 'N':
    default:
      insertAuthInit = false;
      break;
  }
  if (insertAuthInit) {
    // config->insert(
    // "security-client-auth-factory","createUserPasswordAuthInitInstance" );
    // config->insert( "security-client-auth-library","authinitImpl" );
    credentialGeneratorHandler->getAuthInit(config);
  }

  try {
    LOG(" in initclientAuth");
    initClient(true, config);
    LOG(" in initclientAuth 2");
  } catch (...) {
    throw;
  }
}

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define CLIENT3 s2p1
#define LOCATORSERVER s2p2

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateLocator)
  {
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateServer1)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;
    if (credentialGeneratorHandler == NULLPTR) {
      FAIL("credentialGeneratorHandler is NULL");
    }

    try {
      if (isLocalServer) {
        cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
            "authenticator", getXmlPath());
        printf("Input to server cmd is -->  %s",
               cmdServerAuthenticator.c_str());
        CacheHelper::initServer(
            1, NULL, locHostPort,
            const_cast<char*>(cmdServerAuthenticator.c_str()));
        LOG("Server1 started");
      }
    } catch (...) {
      printf("this is some exception");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateServer2)
  {
    std::string cmdServerAuthenticator2;
    cmdServerAuthenticator2 = credentialGeneratorHandler->getServerCmdParams(
        "authenticator", getXmlPath());
    printf("Input to server cmd is -->  %s", cmdServerAuthenticator2.c_str());
    CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml",
                            locHostPort,
                            const_cast<char*>(cmdServerAuthenticator2.c_str()));
    LOG("Server2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    LOG(" 1");
    initCredentialGenerator();
    LOG(" 2");
    try {
      initClientAuth(INCORRECT_CREDENTIALS);
      LOG(" 3");
    } catch (const gemfire::AuthenticationFailedException& other) {
      other.printStackTrace();
      LOG(other.getMessage());
    }

    try {
      LOG(" 4");
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      LOG(" 5");
      // need to insure pool name
      PoolPtr pool = getPool(regionNamesAuth[0]);
      LOG(" 6");
      if (pool != NULLPTR) {
        LOG(" 7");
        RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
        LOG(" 8");
        virtualCache->getRegion(regionNamesAuth[0])->put(keys[0], vals[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }
      FAIL("Should have thrown AuthenticationFailedException.");
    } catch (const gemfire::AuthenticationFailedException& other) {
      other.printStackTrace();
      LOG(other.getMessage());
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      LOG(other.getMessage());
      FAIL("Only AuthenticationFailedException is expected");
    }
    LOG("StepOne Completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
  {
    initClientAuth(CORRECT_CREDENTIALS);
    try {
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      char buff[128] = {'\0'};
      sprintf(buff, "%s_0", regionNamesAuth[0]);
      PoolPtr pool = getPool(regionNamesAuth[0]);
      if (pool != NULLPTR) {
        RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
        RegionPtr virtualRegion = virtualCache->getRegion(regionNamesAuth[0]);
        virtualRegion->create(keys[0], vals[0]);
        virtualRegion->put(keys[0], nvals[0]);
        virtualRegion->containsKeyOnServer(
            gemfire::CacheableKey::create(keys[0]));
        LOG("Operation allowed.");
      } else {
        LOG("Pool is NULL");
      }
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      FAIL(other.getMessage());
    }
    LOG("Handshake  and  Authentication successfully completed");
    LOG("StepTwo Completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepThree)
  {
    initCredentialGenerator();
    initClientAuth(CORRECT_CREDENTIALS);
    try {
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      // need to insure pool name
      PoolPtr pool = getPool(regionNamesAuth[0]);
      if (pool != NULLPTR) {
        RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
        virtualCache->getRegion(regionNamesAuth[0])->put(keys[0], vals[0]);
      } else {
        LOG("Pool is NULL");
      }
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      FAIL(other.getMessage());
    }
    LOG("Handshake  and  Authentication successfully completed");
  }
  LOG("StepThree Completed");
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepFour)
  {
    initCredentialGenerator();
    try {
      initClientAuth(NOT_PROVIDED_CREDENTIALS);
    } catch (const gemfire::AuthenticationRequiredException& other) {
      other.printStackTrace();
      FAIL(other.getMessage());
    }

    try {
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      // need to insure pool name
      PoolPtr pool = getPool(regionNamesAuth[0]);
      if (pool != NULLPTR) {
        RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
        virtualCache->getRegion(regionNamesAuth[0])->put(keys[0], vals[0]);
      } else {
        LOG("Pool is NULL");
      }
      FAIL("Should have thrown AuthenticationRequiredException.");
    } catch (const gemfire::AuthenticationRequiredException& other) {
      other.printStackTrace();
      LOG(other.getMessage());
    } catch (const gemfire::AuthenticationFailedException& other) {
      other.printStackTrace();
      LOG(other.getMessage());
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      LOG(other.getMessage());
      FAIL("Only AuthenticationRequiredException is expected");
    }
    LOG("StepFour Completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFive)
  {
    SLEEP(80);
    try {
      createRegionForSecurity(regionNamesAuth[1], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      // need to insure pool name
      PoolPtr pool = getPool(regionNamesAuth[1]);
      RegionServicePtr virtualCache;
      RegionPtr virtualRegion;
      if (pool != NULLPTR) {
        virtualCache = getVirtualCache(userCreds, pool);
        virtualRegion = virtualCache->getRegion(regionNamesAuth[1]);
      } else {
        LOG("Pool is NULL");
      }
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[0]);
      LOG("before get");
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(virtualRegion->get(keyPtr));
      if (checkPtr != NULLPTR && !strcmp(nvals[0], checkPtr->asChar())) {
        LOG("checkPtr is not null");
        char buf[1024];
        sprintf(buf, "In net search, get returned %s for key %s",
                checkPtr->asChar(), keys[0]);
        LOG(buf);
      } else {
        LOG("checkPtr is NULL");
      }
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      FAIL(other.getMessage());
    }
    LOG("Handshake  and  Authentication successfully completed after FailOver");
    LOG("StepFive Completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSix)
  {
    initClientAuth(CORRECT_CREDENTIALS);
    try {
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      char buff[128] = {'\0'};
      sprintf(buff, "%s_1", regionNamesAuth[0]);
      PoolPtr pool = getPool(regionNamesAuth[0]);
      if (pool != NULLPTR) {
        RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
        RegionPtr virtualRegion = virtualCache->getRegion(regionNamesAuth[0]);
        virtualRegion->create(keys[0], vals[0]);
        virtualRegion->put(keys[0], nvals[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      FAIL(other.getMessage());
    }
    LOG("Handshake  and  Authentication successfully completed");
    LOG("StepSix Completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSeven)
  {
    try {
      initClientAuth(INCORRECT_CREDENTIALS);
    } catch (const gemfire::AuthenticationFailedException& other) {
      other.printStackTrace();
      LOG(other.getMessage());
    }
    LOG("Setting JavaConnectionPoolSize to 0 ");
    CacheHelper::setJavaConnectionPoolSize(0);
    SLEEP(500);
    try {
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      char buff[128] = {'\0'};
      sprintf(buff, "%s_0", regionNamesAuth[0]);
      PoolPtr pool = getPool(regionNamesAuth[0]);
      if (pool != NULLPTR) {
        RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
        RegionPtr virtualRegion = virtualCache->getRegion(regionNamesAuth[0]);
        virtualRegion->create(keys[0], vals[0]);
        virtualRegion->put(keys[0], nvals[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }
      FAIL("Should have thrown AuthenticationFailedException.");
    } catch (const gemfire::AuthenticationFailedException& other) {
      other.printStackTrace();
      LOG(other.getMessage());
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      LOG(other.getMessage());
      FAIL("Only AuthenticationFailedException is expected");
    }
    LOG("StepSeven Completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight)
  {
    initClientAuth(CORRECT_CREDENTIALS);
    try {
      createRegionForSecurity(regionNamesAuth[1], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      // need to insure pool name
      PoolPtr pool = getPool(regionNamesAuth[1]);
      RegionServicePtr virtualCache;
      RegionPtr virtualRegion;
      if (pool != NULLPTR) {
        virtualCache = getVirtualCache(userCreds, pool);
        virtualRegion = virtualCache->getRegion(regionNamesAuth[1]);
      } else {
        LOG("Pool is NULL");
      }

      CacheTransactionManagerPtr txManager =
          getHelper()->getCache()->getCacheTransactionManager();
      LOG("txManager got");
      txManager->begin();
      LOG("txManager begin done");
      virtualRegion->put("TxKey", "TxValue");
      LOG("createEntryTx done");
      txManager->commit();
      LOG("txManager commit done");

      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(virtualRegion->get("TxKey"));
      ASSERT(checkPtr != NULLPTR, "Value not found.");
      LOGINFO("checkPtr->asChar() = %s ", checkPtr->asChar());
      ASSERT(strcmp("TxValue", checkPtr->asChar()) == 0, "Value not correct.");
      if (checkPtr != NULLPTR && !strcmp("TxValue", checkPtr->asChar())) {
        LOG("checkPtr is not null");
        char buf[1024];
        sprintf(buf, "In net search, get returned %s for key %s",
                checkPtr->asChar(), "TxKey");
        LOG(buf);
      } else {
        LOG("checkPtr is NULL");
      }

      txManager->begin();
      LOG("txManager begin done");
      virtualRegion->put("TxKey", "TxNewValue");
      LOG("createEntryTx done");
      txManager->rollback();
      LOG("txManager rollback done");

      checkPtr = dynCast<CacheableStringPtr>(virtualRegion->get("TxKey"));
      ASSERT(checkPtr != NULLPTR, "Value not found.");
      ASSERT(strcmp("TxValue", checkPtr->asChar()) == 0, "Value not correct.");
      if (checkPtr != NULLPTR && !strcmp("TxValue", checkPtr->asChar())) {
        LOG("checkPtr is not null");
        char buf[1024];
        sprintf(buf, "In net search, get returned %s for key %s",
                checkPtr->asChar(), "TxKey");
        LOG(buf);
      } else {
        LOG("checkPtr is NULL");
      }
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      FAIL(other.getMessage());
    }
    LOG("StepEight Completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, CloseCache3)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

void doThinClientSecurityAuthentication() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1);
  CALL_TASK(StepOne);
  CALL_TASK(CreateServer2);
  CALL_TASK(CloseCache1);
  CALL_TASK(StepTwo);
  CALL_TASK(StepThree);
  CALL_TASK(StepFour);
  CALL_TASK(CloseServer1);
  CALL_TASK(StepFive);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(StepSix);
  CALL_TASK(StepSeven);
  CALL_TASK(StepEight);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseCache3);
  CALL_TASK(CloseServer2);
  CALL_TASK(CloseLocator);
}

DUNIT_MAIN
  { doThinClientSecurityAuthentication(); }
END_MAIN
