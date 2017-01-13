/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <string>
#include <stdlib.h>
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/FunctionService.hpp>
#include <gfcpp/Execution.hpp>

#define ROOT_NAME "testThinClientSecurityMultiUserTest"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"
#include "ThinClientHelper.hpp"
#include "ace/Process.h"

#include "ThinClientSecurity.hpp"

using namespace gemfire::testframework::security;
using namespace gemfire;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
CredentialGeneratorPtr credentialGeneratorHandler;

std::string getXmlPath() {
  char xmlPath[1000] = {'\0'};
  const char* path = ACE_OS::getenv("TESTSRC");
  printf(" getXMLPATH = %s \n", path);
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
      credentialGeneratorHandler = CredentialGenerator::create("DUMMY2");
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
  if (loopNum > 1) loopNum = 1;
}
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

#define CLIENT_1 s1p1
#define WRITER_CLIENT s1p2
#define READER_CLIENT s2p1
//#define USER_CLIENT s2p2

const char* regionNamesAuth[] = {"DistRegionAck"};

PropertiesPtr userCreds;
void initClientAuth() {
  userCreds = Properties::create();
  PropertiesPtr config = Properties::create();
  credentialGeneratorHandler->getAuthInit(config);

  credentialGeneratorHandler->getValidCredentials(userCreds);

  try {
    initClient(true, config);
  } catch (...) {
    throw;
  }
}

typedef enum { OP_GET = 0, OP_PUT = 1 } UserOpCode;

class UserThread : public ACE_Task_Base {
  int m_numberOfOps;
  int m_numberOfUsers;
  RegionServicePtr m_userCache;
  RegionPtr m_userRegion;
  int m_userId;
  bool m_failed;
  bool getValidOps;
  int m_totalOpsPassed;

  int getNextOp() {
    return (rand() % 17) % 2;
  }

  int getNextKeyIdx() {
    if (getValidOps) {
      getValidOps = false;
      return m_userId;
    } else {
      getValidOps = true;
    }
    int nextNumber = (rand() % 541) % (m_numberOfUsers + 1);
    if (nextNumber == m_userId) return (nextNumber + 1) % (m_numberOfUsers + 1);
    return nextNumber;
  }

  void getOp() {
    LOG("Get ops");
    bool isPassed = false;
    char key[10] = {'\0'};
    try {
      int nextKey = getNextKeyIdx();

      sprintf(key, "key%d", nextKey);
      char tmp[256] = {'\0'};
      sprintf(tmp, "User is doing get. user id = %d, key = %s", m_userId, key);
      LOG(tmp);
      isPassed = ifUserIdInKey(key);
      m_userRegion->get(key);
      LOG("op got passed");
      m_totalOpsPassed++;
    } catch (const gemfire::NotAuthorizedException&) {
      LOG("NotAuthorizedException Caught");
      if (isPassed) {
        char tmp[256] = {'\0'};
        sprintf(tmp, "Get ops should have passed for user id = %d for key = %s",
                m_userId, key);
        LOG(tmp);
        m_failed = true;
      }
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      m_failed = true;
      char tmp[256] = {'\0'};
      sprintf(tmp, "Some other gemfire exception got for user id = %d",
              m_userId);
      LOG(tmp);
      LOG(other.getMessage());
      m_failed = true;
    } catch (...) {
      m_failed = true;
      char tmp[256] = {'\0'};
      sprintf(tmp, "Some other exception got for user id = %d", m_userId);
      LOG(tmp);
    }
  }

  void putOp() {
    LOG("Put ops");
    bool isPassed = false;
    char key[10] = {'\0'};
    try {
      int nextKey = getNextKeyIdx();

      sprintf(key, "key%d", nextKey);
      char tmp[256] = {'\0'};
      sprintf(tmp, "User is doing put. user id = %d, key = %s", m_userId, key);
      LOG(tmp);
      isPassed = ifUserIdInKey(key);
      m_userRegion->put(key, "val");
      LOG("op got passed");
      m_totalOpsPassed++;
    } catch (const gemfire::NotAuthorizedException&) {
      LOG("NotAuthorizedException Caught");
      if (isPassed) {
        char tmp[256] = {'\0'};
        sprintf(tmp, "Put ops should have passed for user id = %d for key = %s",
                m_userId, key);
        LOG(tmp);
        m_failed = true;
      }
    } catch (const gemfire::Exception& other) {
      other.printStackTrace();
      m_failed = true;
      char tmp[256] = {'\0'};
      sprintf(tmp, "Some other gemfire exception got for user id = %d",
              m_userId);
      LOG(tmp);
      LOG(other.getMessage());
      m_failed = true;
    } catch (...) {
      m_failed = true;
      char tmp[256] = {'\0'};
      sprintf(tmp, "Some other exception got for user id = %d", m_userId);
      LOG(tmp);
    }
  }

  bool ifUserIdInKey(const char* key) {
    std::string s1(key);
    char tmp[10];
    sprintf(tmp, "%d", m_userId);
    std::string userId(tmp);

    size_t found = s1.rfind(userId);
    if (found != std::string::npos) return true;
    return false;
  }

 public:
  UserThread() {
    getValidOps = true;
    m_totalOpsPassed = 0;
  }
  void setParameters(int numberOfOps, int userId, PoolPtr pool,
                     int numberOfUsers) {
    printf("userthread constructor nOo = %d, userid = %d, numberOfUsers = %d\n",
           numberOfOps, userId, numberOfUsers);
    m_userId = userId;
    m_failed = false;
    PropertiesPtr creds = Properties::create();
    char tmp[25] = {'\0'};
    sprintf(tmp, "user%d", userId);

    creds->insert("security-username", tmp);
    creds->insert("security-password", tmp);

    m_numberOfOps = numberOfOps;
    // m_userCache = pool->createSecureUserCache(creds);
    m_userCache = getVirtualCache(creds, pool);
    m_userRegion = m_userCache->getRegion(regionNamesAuth[0]);
    m_numberOfUsers = numberOfUsers;
  }

  void start() { activate(THR_NEW_LWP | THR_JOINABLE); }

  void stop() {
    /*if (m_run) {
       m_run = false;
       wait();
    }*/
  }

  int svc(void) {
    int nOps = 0;
    char key[10] = {'\0'};
    char val[10] = {'\0'};
    printf("User thread first put started\n");
    // users data
    sprintf(key, "key%d", m_userId);
    sprintf(val, "val%d", m_userId);
    printf("User thread first put started key = %s val =%s\n", key, val);
    m_userRegion->put(key, val);
    printf("User thread first put completed\n");
    while (nOps++ < m_numberOfOps && !m_failed) {
      int nextOp = getNextOp();
      switch (nextOp) {
        case 0:
          getOp();
          break;
        case 1:
          putOp();
          break;
        default:
          LOG("Something is worng.");
          break;
      }
    }
    m_userCache->close();
    return 0;
  }

  bool isUserOpsFailed() {
    if (m_failed) {
      char tmp[256] = {'\0'};
      sprintf(tmp, "User ops failed for this user id = %d", m_userId);
      LOG(tmp);
    }
    return m_failed;
  }

  int getTotalOpsPassed() { return m_totalOpsPassed; }
};

DUNIT_TASK_DEFINITION(CLIENT_1, StartServer1)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer:authorizerPP", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          1, "cacheserver_notify_subscription.xml", locHostPort,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_1, StartServer2)
  {
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer:authorizerPP", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          2, "cacheserver_notify_subscription2.xml", locHostPort,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server2 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_1, StartLocator)
  {
    if (isLocator) {
      CacheHelper::initLocator(1);
      LOG("Locator1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_1, StepOne)
  {
    initClientAuth();
    try {
      LOG("Tying Region creation");
      createRegionForSecurity(regionNamesAuth[0], USE_ACK, false,
                              NULLPTR, false, -1, true, 0);
      LOG("Region created successfully");
      PoolPtr pool = getPool(regionNamesAuth[0]);
      int m_numberOfUsers = 10;
      int m_numberOfOps = 100;
      UserThread* uthreads = new UserThread[m_numberOfUsers];

      for (int i = 0; i < m_numberOfUsers; i++) {
        uthreads[i].setParameters(m_numberOfOps, i + 1, pool, m_numberOfUsers);
      }

      LOG("USer created successfully");
      for (int i = 0; i < m_numberOfUsers; i++) {
        uthreads[i].start();
      }
      LOG("USer Threads started");
      for (int i = 0; i < m_numberOfUsers; i++) {
        uthreads[i].wait();
      }
      LOG("USer Thread Completed");
      bool fail = false;
      int totalOpsPassed = 0;
      for (int i = 0; i < m_numberOfUsers; i++) {
        if (uthreads[i].isUserOpsFailed()) {
          fail = true;
        } else {
          totalOpsPassed += uthreads[i].getTotalOpsPassed();
        }
      }

      char tmp[256] = {'\0'};
      sprintf(tmp, "Total ops passed = %d , expected = %d", totalOpsPassed,
              (m_numberOfOps * m_numberOfUsers) / 2);
      printf("%s\n", tmp);
      ASSERT(totalOpsPassed == (m_numberOfOps * m_numberOfUsers) / 2, tmp);
      if (fail) {
        FAIL("User ops failed");
      } else {
        LOG("ALl User ops succed");
      }
    } catch (...) {
      FAIL("Something is worng.");
    }

    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_1, CloseServer1)
  {
    SLEEP(9000);
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_1, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_1, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_1, CloseCacheAdmin)
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
  CALL_TASK(CloseCacheAdmin);
  // CALL_TASK(StepTwo);
  //  CALL_TASK(StartServer2);
  CALL_TASK(CloseServer1);
  // CALL_TASK(StepThree);
  // CALL_TASK(CloseCacheReader);
  // CALL_TASK(CloseCacheWriter);
  // CALL_TASK(CloseCacheAdmin);
  // CALL_TASK(CloseServer2);
  CALL_TASK(CloseLocator);
}

DUNIT_MAIN
  { doThinClientSecurityAuthorization(); }
END_MAIN
