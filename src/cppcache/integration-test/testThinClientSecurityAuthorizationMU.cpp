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

#define ROOT_NAME "testThinClientSecurityAuthenticationMU"
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
char* exFuncNameSendException = (char*)"executeFunction_SendException";

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

PropertiesPtr userCreds;
void initClientAuth(char UserType) {
  userCreds = Properties::create();
  PropertiesPtr config = Properties::create();
  opCodeList wr(tmpWArr, tmpWArr + sizeof tmpWArr / sizeof *tmpWArr);
  opCodeList rt(tmpRArr, tmpRArr + sizeof tmpRArr / sizeof *tmpRArr);
  opCodeList ad(tmpAArr, tmpAArr + sizeof tmpAArr / sizeof *tmpAArr);
  credentialGeneratorHandler->getAuthInit(config);
  switch (UserType) {
    case 'W':
      credentialGeneratorHandler->getAllowedCredentialsForOps(wr, userCreds,
                                                              NULL);
      break;
    case 'R':
      credentialGeneratorHandler->getAllowedCredentialsForOps(rt, userCreds,
                                                              NULL);
      break;
    case 'A':
      credentialGeneratorHandler->getAllowedCredentialsForOps(ad, userCreds,
                                                              NULL);
    default:
      break;
  }

  CacheableStringPtr alias(userCreds->find("security-alias"));
  CacheableStringPtr uname(userCreds->find("security-username"));
  CacheableStringPtr passwd(userCreds->find("security-password"));

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

  try {
    initClient(true, config);
  } catch (...) {
    throw;
  }
}

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StartServer1)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          1, "cacheserver_notify_subscription.xml", locHostPort,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StartServer2)
  {
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          2, "cacheserver_notify_subscription2.xml", locHostPort,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server2 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StartLocator)
  {
    if (isLocator) {
      CacheHelper::initLocator(1);
      LOG("Locator1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, StepOne)
  {
    initClientAuth('A');
    try {
      LOG("Tying Region creation");
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, true, NULLPTR, false,
                              -1, true, 0);
      LOG("Region created successfully");
      LOG("Tying Entry creation");
      RegionServicePtr virtualCache;
      RegionPtr regionPtr;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      LOG(" 6");
      if (pool != NULLPTR) {
        LOG(" 7");
        virtualCache = getVirtualCache(userCreds, pool);
        LOG(" 8");
        regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }

      //---------------------for region clear tests-----
      regionPtr->put(1, 1);
      regionPtr->clear();

      CacheablePtr getVal = regionPtr->get(1);
      if (getVal == NULLPTR) {
        LOG("Get completed after region.clear successfully");
      } else {
        FAIL("Get did not complete successfully");
      }

      //---------------------------------------------------

      // createEntry( regionNamesAuth[0], keys[0], vals[0] );
      regionPtr->create(keys[0], vals[0]);
      LOG("Entry created successfully");
      // updateEntry( regionNamesAuth[0], keys[0], nvals[0] );
      regionPtr->put(keys[0], nvals[0]);
      LOG("Entry updated successfully");
      HashMapOfCacheable entrymap;
      entrymap.clear();
      for (int i = 0; i < 5; i++) {
        entrymap.insert(CacheableKey::create(i), CacheableInt32::create(i));
      }
      // RegionPtr regPtr = getHelper()->getRegion(regionNamesAuth[0]);
      regionPtr->putAll(entrymap);
      LOG("PutAll completed successfully");
      /*for (int i=0; i<5; i++) {
        regPtr->invalidate(CacheableKey::create(i));
      }*/

      LOG("GetServerKeys check started for ADMIN");
      VectorOfCacheableKey keysvec;
      regionPtr->serverKeys(keysvec);
      LOG("GetServerKeys check passed for ADMIN");

      VectorOfCacheableKey entrykeys;
      for (int i = 0; i < 5; i++) {
        entrykeys.push_back(CacheableKey::create(i));
      }
      HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
      valuesMap->clear();
      regionPtr->getAll(entrykeys, valuesMap, NULLPTR, false);
      if (valuesMap->size() > 0) {
        LOG("GetAll completed successfully");
      } else {
        FAIL("GetAll did not complete successfully");
      }
      regionPtr->query("1=1");
      LOG("Query completed successfully");

      QueryServicePtr qs;
      // Using region name as pool name
      try {
        qs = pool->getQueryService();
        FAIL("Pool should not return queryservice in multiusermode");
      } catch (const gemfire::UnsupportedOperationException&) {
        LOG("UnsupportedOperationException Caught for pool.getQuerySerice in "
            "multiusermode");
        LOG("Success");
      } catch (const gemfire::Exception& other) {
        other.printStackTrace();
        FAIL(other.getMessage());
      }

      qs = virtualCache->getQueryService();

      char queryString[100];
      sprintf(queryString, "select * from /%s", regionNamesAuth[0]);
      QueryPtr qry = qs->newQuery(queryString);

      SelectResultsPtr results;
      printf(" before query executing\n");
      results = qry->execute(850);
      LOG("Query completed successfully");

      sprintf(queryString, "select * from /%s", regionNamesAuth[0]);
      CqAttributesFactory cqFac;
      CqAttributesPtr cqAttrs(cqFac.create());
      CqQueryPtr cqQry = qs->newCq("cq_security", queryString, cqAttrs);
      cqQry->execute();
      cqQry->close();
      LOG("CQ completed successfully");

      if (pool != NULLPTR) {
        // TODO:
        // FunctionService::onServer(pool)->execute("securityTest",
        // true)->getResult();
        // FunctionServicePtr funcServ = virtualCache->getFunctionService();
        // funcServ->onServer()->execute("securityTest", true)->getResult();
        FunctionService::onServer(virtualCache)
            ->execute("securityTest", true)
            ->getResult();
        LOG("onServer executed successfully.");
        // funcServ->onServers()->execute("securityTest", true)->getResult();
        FunctionService::onServers(virtualCache)
            ->execute("securityTest", true)
            ->getResult();
        LOG("onServerS executed successfully.");
        FunctionService::onRegion(regionPtr)
            ->execute("securityTest", true)
            ->getResult();
        LOG("FunctionService::onRegion executed successfully.");
        FunctionService::onRegion(regionPtr)->execute("FireNForget", false);
        LOG("Function execution with no result completed successfully");

        //-----------------------Test with
        // sendException-------------------------------//
        LOG("Function execution with sendException");
        char buf[128];
        for (int i = 1; i <= 200; i++) {
          CacheablePtr value(CacheableInt32::create(i));

          sprintf(buf, "execKey-%d", i);
          CacheableKeyPtr key = CacheableKey::create(buf);
          regionPtr->put(key, value);
        }
        LOG("Put for execKey's on region complete.");

        LOG("Adding filter");
        CacheableArrayListPtr arrList = CacheableArrayList::create();
        for (int i = 100; i < 120; i++) {
          sprintf(buf, "execKey-%d", i);
          CacheableKeyPtr key = CacheableKey::create(buf);
          arrList->push_back(key);
        }

        CacheableVectorPtr filter = CacheableVector::create();
        for (int i = 100; i < 120; i++) {
          sprintf(buf, "execKey-%d", i);
          CacheableKeyPtr key = CacheableKey::create(buf);
          filter->push_back(key);
        }
        LOG("Adding filter done.");

        CacheablePtr args = CacheableBoolean::create(1);
        // UNUSED bool getResult = true;

        ExecutionPtr funcExec = FunctionService::onRegion(regionPtr);
        ASSERT(funcExec != NULLPTR, "onRegion Returned NULL");

        ResultCollectorPtr collector =
            funcExec->withArgs(args)->withFilter(filter)->execute(
                exFuncNameSendException, 15);
        ASSERT(collector != NULLPTR, "onRegion collector NULL");

        CacheableVectorPtr result = collector->getResult();

        if (result == NULLPTR) {
          ASSERT(false, "echo String : result is NULL");
        } else {
          try {
            for (int i = 0; i < result->size(); i++) {
              UserFunctionExecutionExceptionPtr uFEPtr =
                  dynCast<UserFunctionExecutionExceptionPtr>(
                      result->operator[](i));
              ASSERT(uFEPtr != NULLPTR, "uFEPtr exception is NULL");
              LOGINFO("Done casting to uFEPtr");
              LOGINFO("Read expected uFEPtr exception %s ",
                      uFEPtr->getMessage()->asChar());
            }
          } catch (ClassCastException& ex) {
            std::string logmsg = "";
            logmsg += ex.getName();
            logmsg += ": ";
            logmsg += ex.getMessage();
            LOG(logmsg.c_str());
            ex.printStackTrace();
            FAIL(
                "exFuncNameSendException casting to string for bool arguement "
                "exception.");
          } catch (...) {
            FAIL(
                "exFuncNameSendException casting to string for bool arguement "
                "Unknown exception.");
          }
        }

        LOG("exFuncNameSendException done for bool arguement.");

        collector = funcExec->withArgs(arrList)->withFilter(filter)->execute(
            exFuncNameSendException, 15);
        ASSERT(collector != NULLPTR, "onRegion collector for arrList NULL");

        result = collector->getResult();
        ASSERT(result->size() == arrList->size() + 1,
               "region get: resultList count is not as arrayList count + "
               "exception");

        for (int i = 0; i < result->size(); i++) {
          try {
            CacheableInt32Ptr intValue =
                dynCast<CacheableInt32Ptr>(result->operator[](i));
            ASSERT(intValue != NULLPTR, "int value is NULL");
            LOGINFO("intValue is %d ", intValue->value());
          } catch (ClassCastException& ex) {
            LOG("exFuncNameSendException casting to int for arrayList "
                "arguement "
                "exception.");
            std::string logmsg = "";
            logmsg += ex.getName();
            logmsg += ": ";
            logmsg += ex.getMessage();
            LOG(logmsg.c_str());
            ex.printStackTrace();
            UserFunctionExecutionExceptionPtr uFEPtr =
                dynCast<UserFunctionExecutionExceptionPtr>(
                    result->operator[](i));
            ASSERT(uFEPtr != NULLPTR, "uFEPtr exception is NULL");
            LOGINFO("Done casting to uFEPtr");
            LOGINFO("Read expected uFEPtr exception %s ",
                    uFEPtr->getMessage()->asChar());
          } catch (...) {
            FAIL(
                "exFuncNameSendException casting to string for bool arguement "
                "Unknown exception.");
          }
        }

        LOG("exFuncNameSendException done for arrayList arguement.");

        LOG("Function execution with sendException successfull");
        //----------------------------------------------------------------------------------------------//

        LOG("Function execution completed successfully");
      } else {
        LOG("Skipping function execution for non pool case");
      }
      // invalidateEntry( regionNamesAuth[0], keys[0] );
      LOG("Entry invalidated successfully");
      // verifyInvalid( regionNamesAuth[0], keys[0] );
      LOG("Entry invalidate-verified successfully");
      // destroyEntry( regionNamesAuth[0], keys[0] );
      regionPtr->destroy(keys[0]);
      LOG("Entry destroyed successfully");
      // verifyDestroyed( regionNamesAuth[0], keys[0] );
      LOG("Entry destroy-verified successfully");
      destroyRegion(regionNamesAuth[0]);
      LOG("Region destroy successfully");
      LOG("Tying Region creation");
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR,
                              false, -1, true, 0);
      char buf[100] = {'\0'};
      static int indexForPool = 0;
      sprintf(buf, "%s_%d", regionNamesAuth[0], indexForPool++);
      pool = getPool(buf);
      LOG(" 6");
      if (pool != NULLPTR) {
        LOG(" 7");
        virtualCache = getVirtualCache(userCreds, pool);
        LOG(" 8");
        regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }

      LOG("Region created successfully");
      // createEntry( regionNamesAuth[0], keys[2], vals[2] );
      regionPtr->create(keys[2], vals[2]);
      LOG("Entry created successfully");
      virtualCache->close();
      LOG("Cache close successfully");
      // RegionPtr regPtr0 = getHelper()->getRegion( regionNamesAuth[0] );
      /*if (regPtr != NULLPTR ) {
        LOG("Going to do registerAllKeys");
       // regPtr->registerAllKeys();
        LOG("Going to do unregisterAllKeys");
       // regPtr->unregisterAllKeys();
      }*/
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION

    try {
      LOG("Trying operation using real region in multiusersecure mode");
      RegionPtr regPtr = getHelper()->getRegion(regionNamesAuth[0]);
      regPtr->put("key", "val");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(WRITER_CLIENT, StepTwo)
  {
    initCredentialGenerator();
    initClientAuth('W');
    try {
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, true, NULLPTR, false,
                              -1, true, 0);
      LOG("Region created successfully");
      RegionServicePtr virtualCache;
      RegionPtr regionPtr;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      LOG(" 6");
      if (pool != NULLPTR) {
        LOG(" 7");
        virtualCache = getVirtualCache(userCreds, pool);
        LOG(" 8");
        regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }

      // createEntry( regionNamesAuth[0], keys[0], vals[0] );
      regionPtr->create(keys[0], vals[0]);
      LOG("Entry created successfully");
      // updateEntry( regionNamesAuth[0], keys[0], nvals[0] );
      regionPtr->put(keys[0], nvals[0]);
      LOG("Entry updated successfully");
      HashMapOfCacheable entrymap;
      entrymap.clear();
      for (int i = 0; i < 5; i++) {
        entrymap.insert(CacheableKey::create(i), CacheableInt32::create(i));
      }
      // RegionPtr regPtr = getHelper()->getRegion(regionNamesAuth[0]);
      regionPtr->putAll(entrymap);
      LOG("PutAll completed successfully");
      // invalidateEntry( regionNamesAuth[0], keys[0] );
      LOG("Entry invalidated successfully");
      // verifyInvalid( regionNamesAuth[0], keys[0] );
      LOG("Entry invalidate-verified successfully");
      // destroyEntry( regionNamesAuth[0], keys[0] );
      regionPtr->destroy(keys[0]);
      LOG("Entry destroyed successfully");
      // verifyDestroyed( regionNamesAuth[0], keys[0] );
      LOG("Entry destroy-verified successfully");
      // createEntry( regionNamesAuth[0], keys[0], vals[0] );
      regionPtr->create(keys[0], vals[0]);
      LOG("Entry created successfully");
      // updateEntry( regionNamesAuth[0], keys[0], nvals[0] );
      regionPtr->put(keys[0], nvals[0]);
      LOG("Entry updated successfully");
      // verifyEntry( regionNamesAuth[0], keys[0], nvals[0] );
      LOG("Entry updation-verified successfully");
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION

    try {
      RegionServicePtr virtualCache;
      RegionPtr regionPtr;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      if (pool != NULLPTR) {
        virtualCache = getVirtualCache(userCreds, pool);
        regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
      } else {
        LOG("Pool is NULL");
      }
      LOG("GetServerKeys check started for WRITER");
      VectorOfCacheableKey keysvec;
      regionPtr->serverKeys(keysvec);
      LOG("GetServerKeys check passed for WRITER");
      FAIL("GetServerKeys should not have completed successfully for WRITER");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      // RegionPtr regPtr0 = getHelper()->getRegion(regionNamesAuth[0]);
      RegionServicePtr virtualCache;
      RegionPtr regionPtr;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      LOG(" 6");
      if (pool != NULLPTR) {
        LOG(" 7");
        virtualCache = getVirtualCache(userCreds, pool);
        LOG(" 8");
        regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[2]);
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(regionPtr->get(keyPtr));
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
    // RegionPtr regPtr0 = getHelper()->getRegion( regionNamesAuth[0] );

    try {
      LOG("Going to do registerAllKeys");
      // regionPtr->registerAllKeys();
      // FAIL("Should not be able to do Register Interest");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      RegionServicePtr virtualCache;
      RegionPtr regionPtr;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      LOG(" 6");
      if (pool != NULLPTR) {
        LOG(" 7");
        virtualCache = getVirtualCache(userCreds, pool);
        LOG(" 8");
        regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }

      /* for (int i=0; i<5; i++) {
         regPtr0->invalidate(CacheableKey::create(i));
       }*/
      VectorOfCacheableKey entrykeys;
      for (int i = 0; i < 5; i++) {
        entrykeys.push_back(CacheableKey::create(i));
      }
      HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
      valuesMap->clear();
      regionPtr->getAll(entrykeys, valuesMap, NULLPTR, false);
      if (valuesMap->size() > 0) {
        FAIL("GetAll should not have completed successfully");
      }
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      RegionServicePtr virtualCache;
      RegionPtr regionPtr;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      LOG(" 6");
      if (pool != NULLPTR) {
        LOG(" 7");
        virtualCache = getVirtualCache(userCreds, pool);
        LOG(" 8");
        regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
        LOG("Operation allowed, something is wrong.");
      } else {
        LOG("Pool is NULL");
      }
      regionPtr->query("1=1");
      FAIL("Query should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    // PoolPtr pool = PoolManager::find(regionNamesAuth[0]);

    try {
      RegionServicePtr virtualCache;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      virtualCache = getVirtualCache(userCreds, pool);
      QueryServicePtr qs = virtualCache->getQueryService();

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
      RegionServicePtr virtualCache;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      virtualCache = getVirtualCache(userCreds, pool);
      // FunctionService::onServer(pool)->execute("securityTest",
      // true)->getResult();
      // FAIL("Function execution should not have completed successfully");
      //  FunctionServicePtr funcServ = virtualCache->getFunctionService();
      // funcServ->onServer()->execute("securityTest", true)->getResult();
      FunctionService::onServer(virtualCache)
          ->execute("securityTest", true)
          ->getResult();
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      RegionServicePtr virtualCache;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      virtualCache = getVirtualCache(userCreds, pool);
      // FunctionService::onServer(pool)->execute("securityTest",
      // true)->getResult();
      // FAIL("Function execution should not have completed successfully");
      // FunctionServicePtr funcServ = virtualCache->getFunctionService();
      // funcServ->onServers()->execute("securityTest", true)->getResult();
      FunctionService::onServers(virtualCache)
          ->execute("securityTest", true)
          ->getResult();
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      RegionServicePtr virtualCache;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      virtualCache = getVirtualCache(userCreds, pool);
      RegionPtr regionPtr;
      regionPtr = virtualCache->getRegion(regionNamesAuth[0]);

      //-----------------------Test with
      // sendException-------------------------------//
      LOG("Function execution with sendException with expected Authorization "
          "exception");
      char buf[128];
      for (int i = 1; i <= 200; i++) {
        CacheablePtr value(CacheableInt32::create(i));

        sprintf(buf, "execKey-%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        regionPtr->put(key, value);
      }
      LOG("Put for execKey's on region complete.");

      LOG("Adding filter");
      CacheableArrayListPtr arrList = CacheableArrayList::create();
      for (int i = 100; i < 120; i++) {
        sprintf(buf, "execKey-%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        arrList->push_back(key);
      }

      CacheableVectorPtr filter = CacheableVector::create();
      for (int i = 100; i < 120; i++) {
        sprintf(buf, "execKey-%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        filter->push_back(key);
      }
      LOG("Adding filter done.");

      CacheablePtr args = CacheableBoolean::create(1);
      // UNUSED bool getResult = true;

      LOG("OnServers with sendException");

      ExecutionPtr funcExec = FunctionService::onServers(virtualCache);

      ResultCollectorPtr collector =
          funcExec->withArgs(args)->execute(exFuncNameSendException, 15);

      //----------------------------------------------------------------------------------------------//
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      RegionServicePtr virtualCache;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      virtualCache = getVirtualCache(userCreds, pool);
      RegionPtr regionPtr;
      regionPtr = virtualCache->getRegion(regionNamesAuth[0]);

      // FunctionService::onServer(pool)->execute("securityTest",
      // true)->getResult();
      // FAIL("Function execution should not have completed successfully");
      FunctionService::onRegion(regionPtr)
          ->execute("securityTest", true)
          ->getResult();
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      RegionServicePtr virtualCache;
      PoolPtr pool = getPool(regionNamesAuth[0]);
      virtualCache = getVirtualCache(userCreds, pool);
      RegionPtr regionPtr;
      regionPtr = virtualCache->getRegion(regionNamesAuth[0]);

      //-----------------------Test with
      // sendException-------------------------------//
      LOG("Function execution with sendException with expected Authorization "
          "exception with onRegion");
      char buf[128];
      for (int i = 1; i <= 200; i++) {
        CacheablePtr value(CacheableInt32::create(i));

        sprintf(buf, "execKey-%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        regionPtr->put(key, value);
      }
      LOG("Put for execKey's on region complete.");

      LOG("Adding filter");
      CacheableArrayListPtr arrList = CacheableArrayList::create();
      for (int i = 100; i < 120; i++) {
        sprintf(buf, "execKey-%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        arrList->push_back(key);
      }

      CacheableVectorPtr filter = CacheableVector::create();
      for (int i = 100; i < 120; i++) {
        sprintf(buf, "execKey-%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        filter->push_back(key);
      }
      LOG("Adding filter done.");

      CacheablePtr args = CacheableBoolean::create(1);
      // UNUSED bool getResult = true;

      LOG("OnServers with sendException");

      ExecutionPtr funcExec = FunctionService::onRegion(regionPtr);

      ResultCollectorPtr collector =
          funcExec->withArgs(args)->withFilter(filter)->execute(
              exFuncNameSendException, 15);

      //----------------------------------------------------------------------------------------------//

      // FunctionService::onServer(pool)->execute("securityTest",
      // true)->getResult();
      // FAIL("Function execution should not have completed successfully");
      // FunctionService::onRegion(regionPtr)->execute("securityTest",
      // true)->getResult();
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    RegionServicePtr virtualCache;
    RegionPtr regionPtr;
    PoolPtr pool = getPool(regionNamesAuth[0]);
    LOG(" 6");
    if (pool != NULLPTR) {
      LOG(" 7");
      virtualCache = getVirtualCache(userCreds, pool);
      LOG(" 8");
      regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
      LOG("Operation allowed, something is wrong.");
    } else {
      LOG("Pool is NULL");
    }

    // createEntry( regionNamesAuth[0], keys[2], vals[2] );
    regionPtr->create(keys[2], vals[2]);
    LOG("Entry created successfully");

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(READER_CLIENT, StepThree)
  {
    initCredentialGenerator();
    initClientAuth('R');
    char buf[100];
    int i = 102;

    createRegionForSecurity(regionNamesAuth[0], USE_ACK, false, NULLPTR, false,
                            -1, true, 0);
    RegionServicePtr virtualCache;
    RegionPtr rptr;
    PoolPtr pool = getPool(regionNamesAuth[0]);
    LOG(" 6");
    if (pool != NULLPTR) {
      LOG(" 7");
      virtualCache = getVirtualCache(userCreds, pool);
      LOG(" 8");
      rptr = virtualCache->getRegion(regionNamesAuth[0]);
      LOG("Operation allowed, something is wrong.");
    } else {
      LOG("Pool is NULL");
    }

    // rptr = getHelper()->getRegion(regionNamesAuth[0]);
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
      rptr->create(keys[2], vals[2]);
      FAIL("Should have got NotAuthorizedException during createEntry");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      LOG("Trying region clear..");
      rptr->clear();
      FAIL("Should have got NotAuthorizedException for region.clear ops");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    // ASSERT(!rptr->containsKey(keys[2]),   "Key should not have been found in
    // the region");

    try {
      LOG("Trying updateEntry");
      // updateEntry(regionNamesAuth[0], keys[2], nvals[2], false, false);
      rptr->put(keys[2], nvals[2]);
      FAIL("Should have got NotAuthorizedException during updateEntry");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    // ASSERT(!rptr->containsKey(keys[2]),  "Key should not have been found in
    // the
    // region");

    try {
      // RegionPtr regPtr0 = getHelper()->getRegion(regionNamesAuth[0]);
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[2]);
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(rptr->get(keyPtr));
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

    // RegionPtr regPtr0 = getHelper()->getRegion( regionNamesAuth[0] );
    if (rptr != NULLPTR) {
      try {
        LOG("Going to do registerAllKeys");
        //  rptr->registerAllKeys();
        LOG("Going to do unregisterAllKeys");
        //  rptr->unregisterAllKeys();
      }
      HANDLE_NO_NOT_AUTHORIZED_EXCEPTION
    }

    try {
      HashMapOfCacheable entrymap;
      entrymap.clear();
      for (int i = 0; i < 5; i++) {
        entrymap.insert(CacheableKey::create(i), CacheableInt32::create(i));
      }
      rptr->putAll(entrymap);
      FAIL("PutAll should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      LOG("GetServerKeys check started for READER");
      VectorOfCacheableKey keysvec;
      rptr->serverKeys(keysvec);
      LOG("GetServerKeys check passed for READER");
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION

    try {
      VectorOfCacheableKey entrykeys;
      for (int i = 0; i < 5; i++) {
        entrykeys.push_back(CacheableKey::create(i));
      }
      HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
      valuesMap->clear();
      rptr->getAll(entrykeys, valuesMap, NULLPTR, false);
      if (valuesMap->size() > 0) {
        LOG("GetAll completed successfully");
      } else {
        FAIL("GetAll did not complete successfully");
      }
    }
    HANDLE_NO_NOT_AUTHORIZED_EXCEPTION

    try {
      rptr->query("1=1");

      FAIL("Query should not have completed successfully");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    // PoolPtr pool = PoolManager::find(regionNamesAuth[0]);

    try {
      /*QueryServicePtr qs;
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
      FAIL("CQ should not have completed successfully");*/
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION

    try {
      // FunctionService::onServer(pool)->execute("securityTest",
      // true)->getResult();
      // FAIL("Function execution should not have completed successfully");
      // FunctionServicePtr funcServ = virtualCache->getFunctionService();
      // funcServ->onServer()->execute("securityTest", true)->getResult();
      FunctionService::onServer(virtualCache)
          ->execute("securityTest", true)
          ->getResult();
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

DUNIT_TASK_DEFINITION(ADMIN_CLIENT, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
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

void doThinClientSecurityAuthorization() {
  CALL_TASK(StartLocator);
  CALL_TASK(StartServer1);
  CALL_TASK(StepOne);
  CALL_TASK(StepTwo);
  CALL_TASK(StartServer2);
  CALL_TASK(CloseServer1);
  CALL_TASK(StepThree);
  CALL_TASK(CloseCacheReader);
  CALL_TASK(CloseCacheWriter);
  CALL_TASK(CloseCacheAdmin);
  CALL_TASK(CloseServer2);
  CALL_TASK(CloseLocator);
}

DUNIT_MAIN
  { doThinClientSecurityAuthorization(); }
END_MAIN
