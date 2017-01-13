/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef THINCLIENTTRANSACTIONS_HPP_
#define THINCLIENTTRANSACTIONS_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/Auto_Event.h>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <string>

#define ROOT_NAME "ThinClientTransactions"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2
#define CREATE_TWICE_KEY "__create_twice_key"
#define CREATE_TWICE_VALUE "__create_twice_value"
CacheHelper* cacheHelper = NULL;
static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;

const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

bool g_isGridClient = false;

DUNIT_TASK_DEFINITION(CLIENT1, Alter_Client_Grid_Property_1)
  { g_isGridClient = !g_isGridClient; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Alter_Client_Grid_Property_2)
  { g_isGridClient = !g_isGridClient; }
END_TASK_DEFINITION

void initClient(const bool isthinClient) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    if (g_isGridClient) {
      config->insert("grid-client", "true");
    }
    config->insert("suspended-tx-timeout", "1");
    cacheHelper = new CacheHelper(isthinClient, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void cleanProc() {
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}

CacheHelper* getHelper() {
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}

void _verifyEntry(const char* name, const char* key, const char* val,
                  bool noKey) {
  // Verify key and value exist in this region, in this process.
  const char* value = (val == 0) ? "" : val;
  char* buf = (char*)malloc(1024 + strlen(key) + strlen(value));
  ASSERT(buf, "Unable to malloc buffer for logging.");
  if (noKey)
    sprintf(buf, "Verify key %s does not exist in region %s", key, name);
  else if (val == 0)
    sprintf(buf, "Verify value for key %s does not exist in region %s", key,
            name);
  else
    sprintf(buf, "Verify value for key %s is: %s in region %s", key, value,
            name);
  LOG(buf);
  free(buf);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableKeyPtr keyPtr = createKey(key);

  // if the region is no ack, then we may need to wait...
  if (noKey == false) {  // need to find the key!
    ASSERT(regPtr->containsKey(keyPtr), "Key not found in region.");
  }
  if (val != NULL) {  // need to have a value!
    ASSERT(regPtr->containsValueForKey(keyPtr), "Value not found in region.");
  }

  // loop up to MAX times, testing condition
  uint32_t MAX = 100;
  uint32_t SLEEP = 10;  // milliseconds
  uint32_t containsKeyCnt = 0;
  uint32_t containsValueCnt = 0;
  uint32_t testValueCnt = 0;

  for (int i = MAX; i >= 0; i--) {
    if (noKey) {
      if (regPtr->containsKey(keyPtr))
        containsKeyCnt++;
      else
        break;
      ASSERT(containsKeyCnt < MAX, "Key found in region.");
    }
    if (val == NULL) {
      if (regPtr->containsValueForKey(keyPtr))
        containsValueCnt++;
      else
        break;
      ASSERT(containsValueCnt < MAX, "Value found in region.");
    }

    if (val != NULL) {
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(regPtr->get(keyPtr));

      ASSERT(checkPtr != NULLPTR, "Value Ptr should not be null.");
      char buf[1024];
      sprintf(buf, "In verify loop, get returned %s for key %s",
              checkPtr->asChar(), key);
      LOG(buf);
      if (strcmp(checkPtr->asChar(), value) != 0) {
        testValueCnt++;
      } else {
        break;
      }
      ASSERT(testValueCnt < MAX, "Incorrect value found.");
    }
    dunit::sleep(SLEEP);
  }
}

#define verifyInvalid(x, y) _verifyInvalid(x, y, __LINE__)

void _verifyInvalid(const char* name, const char* key, int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyInvalid() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, 0, false);
  LOG("Entry invalidated.");
}

#define verifyDestroyed(x, y) _verifyDestroyed(x, y, __LINE__)

void _verifyDestroyed(const char* name, const char* key, int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyDestroyed() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, 0, true);
  LOG("Entry destroyed.");
}

#define verifyEntry(x, y, z) _verifyEntry(x, y, z, __LINE__)

void _verifyEntry(const char* name, const char* key, const char* val,
                  int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyEntry() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, val, false);
  LOG("Entry verified.");
}

void createRegion(const char* name, bool ackMode, const char* endpoints,
                  bool clientNotificationEnabled = false,
                  bool cachingEnable = true) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createRegion(name, ackMode, cachingEnable, NULLPTR,
                                endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}
void createPooledRegion(const char* name, bool ackMode, const char* locators,
                        const char* poolname,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createPooledRegion(name, ackMode, locators, poolname,
                                      cachingEnable, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}

void createPooledRegionSticky(const char* name, bool ackMode,
                              const char* locators, const char* poolname,
                              bool clientNotificationEnabled = false,
                              bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createPooledRegionSticky(
      name, ackMode, locators, poolname, cachingEnable,
      clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}

void createEntry(const char* name, const char* key, const char* value) {
  LOG("createEntry() entered.");
  fprintf(stdout, "Creating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Create entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(!regPtr->containsKey(keyPtr),
         "Key should not have been found in region.");
  ASSERT(!regPtr->containsValueForKey(keyPtr),
         "Value should not have been found in region.");

  // regPtr->create( keyPtr, valPtr );
  regPtr->put(keyPtr, valPtr);
  LOG("Created entry.");

  // verifyEntry( name, key, value );
  LOG("Entry created.");
}
void createEntryTwice(const char* name, const char* key, const char* value) {
  LOG("createEntryTwice() entered.");
  char message[500];
  sprintf(message, "Creating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  LOG(message);
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);
  RegionPtr regPtr = getHelper()->getRegion(name);
  regPtr->create(keyPtr, valPtr);
  try {
    regPtr->create(keyPtr, valPtr);
  } catch (const EntryExistsException& gemfireExcp) {
    LOG(gemfireExcp.getMessage());
    LOG("createEntryTwice() Clean Exit.");
    return;
  }
  ASSERT(false,
         "Creating key twice is not allowed and while doing that exception was "
         "not thrown");
  return;  // This return will never reach
}

void updateEntry(const char* name, const char* key, const char* value) {
  LOG("updateEntry() entered.");
  fprintf(stdout, "Updating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Update entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(regPtr->containsKey(keyPtr), "Key should have been found in region.");
  ASSERT(regPtr->containsValueForKey(keyPtr),
         "Value should have been found in region.");

  regPtr->put(keyPtr, valPtr);
  LOG("Put entry.");

  verifyEntry(name, key, value);
  LOG("Entry updated.");
}

void doGetAgain(const char* name, const char* key, const char* value) {
  LOG("doGetAgain() entered.");
  fprintf(stdout,
          "get for entry -- key: %s  expecting value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  fprintf(stdout, "get  region name%s\n", regPtr->getName());
  fflush(stdout);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableStringPtr checkPtr =
      dynCast<CacheableStringPtr>(regPtr->get(keyPtr));  // force a netsearch

  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
    char buf[1024];
    sprintf(buf, "In doGetAgain, get returned %s for key %s",
            checkPtr->asChar(), key);
    LOG(buf);
  } else {
    LOG("checkPtr is NULL");
  }
  verifyEntry(name, key, value);
  LOG("GetAgain complete.");
}

void doNetsearch(const char* name, const char* key, const char* value) {
  LOG("doNetsearch() entered.");
  fprintf(
      stdout,
      "Netsearching for entry -- key: %s  expecting value: %s in region %s\n",
      key, value, name);
  fflush(stdout);
  static int count = 0;
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  fprintf(stdout, "netsearch  region %s\n", regPtr->getName());
  fflush(stdout);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  if (count == 0) {
    ASSERT(!regPtr->containsKey(keyPtr),
           "Key should not have been found in region.");
    ASSERT(!regPtr->containsValueForKey(keyPtr),
           "Value should not have been found in region.");
    count++;
  }
  CacheableStringPtr checkPtr =
      dynCast<CacheableStringPtr>(regPtr->get(keyPtr));  // force a netsearch

  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
    char buf[1024];
    sprintf(buf, "In net search, get returned %s for key %s",
            checkPtr->asChar(), key);
    LOG(buf);
  } else {
    LOG("checkPtr is NULL");
  }
  verifyEntry(name, key, value);
  LOG("Netsearch complete.");
}

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4",
                      "Key-5", "Key-6", "Key-7"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4",
                      "Value-5", "Value-6", "Value-7"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4", "New Value-5", "New Value-6",
                       "New Value-7"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck", "testregion"};

const bool USE_ACK = true;
const bool NO_ACK = false;
#include "LocatorHelper.hpp"
#define THREADERRORCHECK(x, y) \
  if (!(x)) {                  \
    m_isFailed = true;         \
    sprintf(m_error, y);       \
    return -1;                 \
  }

class SuspendTransactionThread : public ACE_Task_Base {
 private:
  TransactionIdPtr m_suspendedTransaction;
  bool m_sleep;
  ACE_Auto_Event* m_txEvent;

 public:
  SuspendTransactionThread(bool sleep, ACE_Auto_Event* txEvent)
      : m_suspendedTransaction(NULLPTR), m_sleep(sleep), m_txEvent(txEvent) {}

  int svc(void) {
    char buf[1024];
    sprintf(buf, " In SuspendTransactionThread");
    LOG(buf);

    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();

    txManager->begin();

    createEntry(regionNames[0], keys[4], vals[4]);
    createEntry(regionNames[1], keys[5], vals[5]);

    m_suspendedTransaction = txManager->getTransactionId();

    if (m_sleep) {
      m_txEvent->wait();
      ACE_OS::sleep(5);
    }

    m_suspendedTransaction = txManager->suspend();
    sprintf(buf, " Out SuspendTransactionThread");
    LOG(buf);

    PoolManager::find("__TESTPOOL1_")->releaseThreadLocalConnection();

    return 0;
  }
  void start() { activate(); }
  void stop() { wait(); }
  TransactionIdPtr getSuspendedTx() { return m_suspendedTransaction; }
};
class ResumeTransactionThread : public ACE_Task_Base {
 private:
  TransactionIdPtr m_suspendedTransaction;
  bool m_commit;
  bool m_tryResumeWithSleep;
  bool m_isFailed;
  char m_error[256];
  ACE_Auto_Event* m_txEvent;

 public:
  ResumeTransactionThread(TransactionIdPtr suspendedTransaction, bool commit,
                          bool tryResumeWithSleep, ACE_Auto_Event* txEvent)
      : m_suspendedTransaction(suspendedTransaction),
        m_commit(commit),
        m_tryResumeWithSleep(tryResumeWithSleep),
        m_isFailed(false),
        m_txEvent(txEvent) {}

  int svc(void) {
    char buf[1024];
    sprintf(buf, "In ResumeTransactionThread");
    LOG(buf);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    THREADERRORCHECK(regPtr0 != NULLPTR,
                     "In ResumeTransactionThread - Region not found.");

    CacheableKeyPtr keyPtr4 = createKey(keys[4]);
    CacheableKeyPtr keyPtr5 = createKey(keys[5]);
    CacheableKeyPtr keyPtr6 = createKey(keys[6]);

    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    THREADERRORCHECK(regPtr1 != NULLPTR,
                     "In ResumeTransactionThread - Region not found.");

    THREADERRORCHECK(!regPtr0->containsKeyOnServer(keyPtr4),
                     "In ResumeTransactionThread - Key should not have been "
                     "found in region.");

    THREADERRORCHECK(!regPtr1->containsKeyOnServer(keyPtr5),
                     "In ResumeTransactionThread - Key should not have been "
                     "found in region.");

    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();
    if (m_tryResumeWithSleep) {
      THREADERRORCHECK(!txManager->isSuspended(m_suspendedTransaction),
                       "In ResumeTransactionThread - the transaction should "
                       "NOT be in suspended state");
    } else {
      THREADERRORCHECK(txManager->isSuspended(m_suspendedTransaction),
                       "In ResumeTransactionThread - the transaction should be "
                       "in suspended state");
    }

    THREADERRORCHECK(
        txManager->exists(m_suspendedTransaction),
        "In ResumeTransactionThread - the transaction should exist");

    if (m_tryResumeWithSleep) {
      m_txEvent->signal();
      txManager->tryResume(m_suspendedTransaction, 30000);
    } else {
      txManager->resume(m_suspendedTransaction);
    }

    THREADERRORCHECK(
        regPtr0->containsKeyOnServer(keyPtr4),
        "In ResumeTransactionThread - Key should have been found in region.");
    THREADERRORCHECK(
        regPtr1->containsKeyOnServer(keyPtr5),
        "In ResumeTransactionThread - Key should have been found in region.");

    createEntry(regionNames[1], keys[6], vals[6]);

    if (m_commit) {
      txManager->commit();
      THREADERRORCHECK(
          regPtr0->containsKeyOnServer(keyPtr4),
          "In ResumeTransactionThread - Key should have been found in region.");
      THREADERRORCHECK(
          regPtr1->containsKeyOnServer(keyPtr5),
          "In ResumeTransactionThread - Key should have been found in region.");
      THREADERRORCHECK(
          regPtr1->containsKeyOnServer(keyPtr6),
          "In ResumeTransactionThread - Key should have been found in region.");

    } else {
      txManager->rollback();
      THREADERRORCHECK(!regPtr0->containsKeyOnServer(keyPtr4),
                       "In ResumeTransactionThread - Key should not have been "
                       "found in region.");
      THREADERRORCHECK(!regPtr1->containsKeyOnServer(keyPtr5),
                       "In ResumeTransactionThread - Key should not have been "
                       "found in region.");
      THREADERRORCHECK(!regPtr1->containsKeyOnServer(keyPtr6),
                       "In ResumeTransactionThread - Key should not have been "
                       "found in region.");
    }

    if (m_commit) {
      regPtr1->destroy(keyPtr6);
      regPtr1->destroy(keyPtr5);
      regPtr0->destroy(keyPtr4);

      THREADERRORCHECK(!regPtr1->containsKeyOnServer(keyPtr6),
                       "In ResumeTransactionThread - Key should not have been "
                       "found in region.");
      THREADERRORCHECK(!regPtr1->containsKeyOnServer(keyPtr5),
                       "In ResumeTransactionThread - Key should not have been "
                       "found in region.");
      THREADERRORCHECK(!regPtr0->containsKeyOnServer(keyPtr4),
                       "In ResumeTransactionThread - Key should not have been "
                       "found in region.");
    } else {
      try {
        regPtr1->destroy(keyPtr6);
        FAIL("Should have got EntryNotFoundException for keyPtr6");
      } catch (EntryNotFoundException& /*ex*/) {
        LOG("Got expected EntryNotFoundException for keyPtr6");
      }
      try {
        regPtr1->destroy(keyPtr5);
        FAIL("Should have got EntryNotFoundException for keyPtr5");
      } catch (EntryNotFoundException& /*ex*/) {
        LOG("Got expected EntryNotFoundException for keyPtr5");
      }
      try {
        regPtr0->destroy(keyPtr4);
        FAIL("Should have got EntryNotFoundException for keyPtr4");
      } catch (EntryNotFoundException& /*ex*/) {
        LOG("Got expected EntryNotFoundException for keyPtr4");
      }
    }
    PoolManager::find("__TESTPOOL1_")->releaseThreadLocalConnection();
    sprintf(buf, " Out ResumeTransactionThread");
    LOG(buf);
    return 0;
  }
  void start() { activate(); }
  void stop() { wait(); }
  bool isFailed() { return m_isFailed; }
  char* getError() { return m_error; }
};

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SuspendResumeCommit)
  {
    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    ASSERT(regPtr0 != NULLPTR, "In SuspendResumeCommit - Region not found.");
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    ASSERT(regPtr1 != NULLPTR, "In SuspendResumeCommit - Region not found.");
    CacheableKeyPtr keyPtr4 = createKey(keys[4]);
    CacheableKeyPtr keyPtr5 = createKey(keys[5]);
    CacheableKeyPtr keyPtr6 = createKey(keys[6]);

    txManager->begin();
    createEntry(regionNames[0], keys[4], vals[4]);
    createEntry(regionNames[1], keys[5], vals[5]);
    TransactionIdPtr m_suspendedTransaction = txManager->suspend();

    ASSERT(
        !regPtr0->containsKeyOnServer(keyPtr4),
        "In SuspendResumeCommit - Key should not have been found in region.");
    ASSERT(
        !regPtr1->containsKeyOnServer(keyPtr5),
        "In SuspendResumeCommit - Key should not have been found in region.");

    ASSERT(txManager->isSuspended(m_suspendedTransaction),
           "In SuspendResumeCommit - the transaction should be in suspended "
           "state");
    ASSERT(txManager->exists(m_suspendedTransaction),
           "In SuspendResumeCommit - the transaction should exist");

    txManager->resume(m_suspendedTransaction);

    ASSERT(
        !txManager->tryResume(m_suspendedTransaction),
        "SuspendResumeRollback: Transaction shouldnt have been resumed again");
    ASSERT(regPtr0->containsKeyOnServer(keyPtr4),
           "In SuspendResumeCommit - Key should have been found in region.");
    ASSERT(regPtr1->containsKeyOnServer(keyPtr5),
           "In SuspendResumeCommit - Key should have been found in region.");

    createEntry(regionNames[1], keys[6], vals[6]);

    txManager->commit();
    ASSERT(regPtr0->containsKeyOnServer(keyPtr4),
           "In SuspendResumeCommit - Key should have been found in region.");
    ASSERT(regPtr1->containsKeyOnServer(keyPtr5),
           "In SuspendResumeCommit - Key should have been found in region.");
    ASSERT(regPtr1->containsKeyOnServer(keyPtr6),
           "In SuspendResumeCommit - Key should have been found in region.");

    regPtr1->destroy(keyPtr6);
    regPtr1->destroy(keyPtr5);
    regPtr0->destroy(keyPtr4);

    ASSERT(!txManager->isSuspended(m_suspendedTransaction),
           "In SuspendResumeCommit the transaction should NOT present");
    ASSERT(!txManager->exists(m_suspendedTransaction),
           "In SuspendResumeCommit - the transaction should NOT exist");
    ASSERT(!txManager->tryResume(m_suspendedTransaction),
           "In SuspendResumeCommit - the transaction should NOT have been "
           "resumed");

    bool resumeExc = false;
    try {
      txManager->resume(m_suspendedTransaction);
    } catch (const IllegalStateException) {
      resumeExc = true;
    }

    ASSERT(resumeExc,
           "SuspendResumeCommit: Transaction shouldnt have been resumed");

    ASSERT(txManager->suspend() == NULLPTR,
           "SuspendResumeCommit: Transaction shouldnt have been suspended");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SuspendTimeOut)
  {
    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();
    CacheableKeyPtr keyPtr4 = createKey(keys[4]);
    CacheableKeyPtr keyPtr5 = createKey(keys[5]);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    ASSERT(regPtr0 != NULLPTR, "In SuspendTimeOut - Region not found.");

    txManager->begin();
    createEntry(regionNames[0], keys[4], vals[4]);
    TransactionIdPtr tid1 = txManager->suspend();

    txManager->begin();
    createEntry(regionNames[0], keys[5], vals[5]);
    TransactionIdPtr tid2 = txManager->suspend();

    txManager->resume(tid1);
    createEntry(regionNames[0], keys[6], vals[6]);
    txManager->commit();

    ASSERT(txManager->isSuspended(tid2),
           "In SuspendTimeOut the transaction should be present");
    ASSERT(txManager->exists(tid2),
           "In SuspendTimeOut - the transaction should exist");

    ACE_OS::sleep(65);
    ASSERT(!txManager->tryResume(tid2),
           "In SuspendTimeOut - the transaction should NOT have been resumed");
    ASSERT(!txManager->isSuspended(tid2),
           "In SuspendTimeOut the transaction should NOT present");
    ASSERT(!txManager->exists(tid2),
           "In SuspendTimeOut - the transaction should NOT exist");
    ASSERT(regPtr0->containsKeyOnServer(keyPtr4),
           "In SuspendTimeOut - Key should have been found in region.");
    ASSERT(!regPtr0->containsKeyOnServer(keyPtr5),
           "In SuspendTimeOut - Key should not have been found in region.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SuspendResumeRollback)
  {
    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();
    CacheableKeyPtr keyPtr4 = createKey(keys[4]);
    CacheableKeyPtr keyPtr5 = createKey(keys[5]);
    CacheableKeyPtr keyPtr6 = createKey(keys[6]);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    ASSERT(regPtr0 != NULLPTR, "In SuspendResumeRollback - Region not found.");
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    ASSERT(regPtr1 != NULLPTR, "In SuspendResumeRollback - Region not found.");

    txManager->begin();
    createEntry(regionNames[0], keys[4], vals[4]);
    createEntry(regionNames[1], keys[5], vals[5]);
    TransactionIdPtr m_suspendedTransaction = txManager->suspend();

    ASSERT(
        !regPtr0->containsKeyOnServer(keyPtr4),
        "In SuspendResumeRollback - Key should not have been found in region.");
    ASSERT(
        !regPtr1->containsKeyOnServer(keyPtr5),
        "In SuspendResumeRollback - Key should not have been found in region.");

    ASSERT(txManager->isSuspended(m_suspendedTransaction),
           "In SuspendResumeRollback the transaction should be in suspended "
           "state");
    ASSERT(txManager->exists(m_suspendedTransaction),
           "In SuspendResumeRollback - the transaction should exist");

    txManager->resume(m_suspendedTransaction);

    ASSERT(regPtr0->containsKeyOnServer(keyPtr4),
           "In SuspendResumeRollback - Key should have been found in region.");
    ASSERT(regPtr1->containsKeyOnServer(keyPtr5),
           "In SuspendResumeRollback - Key should have been found in region.");

    createEntry(regionNames[1], keys[6], vals[6]);

    txManager->rollback();
    ASSERT(
        !regPtr0->containsKeyOnServer(keyPtr4),
        "In SuspendResumeRollback - Key should not have been found in region.");
    ASSERT(
        !regPtr1->containsKeyOnServer(keyPtr5),
        "In SuspendResumeRollback - Key should not have been found in region.");
    ASSERT(
        !regPtr1->containsKeyOnServer(keyPtr6),
        "In SuspendResumeRollback - Key should not have been found in region.");

    try {
      regPtr1->destroy(keyPtr6);
      FAIL("Should have got EntryNotFoundException for keyPtr6");
    } catch (EntryNotFoundException& /*ex*/) {
      LOG("Got expected EntryNotFoundException for keyPtr6");
    }
    try {
      regPtr1->destroy(keyPtr5);
      FAIL("Should have got EntryNotFoundException for keyPtr5");
    } catch (EntryNotFoundException& /*ex*/) {
      LOG("Got expected EntryNotFoundException for keyPtr5");
    }
    try {
      regPtr0->destroy(keyPtr4);
      FAIL("Should have got EntryNotFoundException for keyPtr4");
    } catch (EntryNotFoundException& /*ex*/) {
      LOG("Got expected EntryNotFoundException for keyPtr4");
    }

    ASSERT(!txManager->isSuspended(m_suspendedTransaction),
           "In SuspendResumeRollback the transaction should NOT present");
    ASSERT(!txManager->exists(m_suspendedTransaction),
           "In SuspendResumeRollback - the transaction should NOT exist");
    ASSERT(!txManager->tryResume(m_suspendedTransaction),
           "In SuspendResumeRollback - the transaction should NOT have been "
           "resumed");
    bool resumeExc = false;
    try {
      txManager->resume(m_suspendedTransaction);
    } catch (const IllegalStateException) {
      resumeExc = true;
    }

    ASSERT(resumeExc,
           "SuspendResumeRollback: Transaction shouldnt have been resumed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, SuspendResumeInThread)
  {
    // start suspend thread  and resume thread and rollback immedidately
    char buf[1024];
    sprintf(
        buf,
        "start suspend thread  and resume thread and rollback immedidately");
    LOG(buf);
    ACE_Auto_Event txEvent;

    SuspendTransactionThread* suspendTh =
        new SuspendTransactionThread(false, &txEvent);
    suspendTh->activate();
    ACE_OS::sleep(2);
    ResumeTransactionThread* resumeTh = new ResumeTransactionThread(
        suspendTh->getSuspendedTx(), false, false, &txEvent);
    resumeTh->activate();

    suspendTh->wait();
    delete suspendTh;
    resumeTh->wait();
    ASSERT(!resumeTh->isFailed(), resumeTh->getError());
    delete resumeTh;

    // start suspend thread  and resume thread and commit immedidately
    sprintf(buf,
            "start suspend thread  and resume thread and commit immedidately");
    LOG(buf);
    suspendTh = new SuspendTransactionThread(false, &txEvent);
    suspendTh->activate();
    ACE_OS::sleep(2);
    resumeTh = new ResumeTransactionThread(suspendTh->getSuspendedTx(), true,
                                           false, &txEvent);
    resumeTh->activate();

    suspendTh->wait();
    delete suspendTh;
    resumeTh->wait();
    ASSERT(!resumeTh->isFailed(), resumeTh->getError());
    delete resumeTh;

    // start suspend thread  and tryresume thread with rollback. make tryResume
    // to
    // sleep
    sprintf(buf,
            "start suspend thread  and tryresume thread with rollback. make "
            "tryResume to sleep");
    LOG(buf);
    suspendTh = new SuspendTransactionThread(true, &txEvent);
    suspendTh->activate();
    ACE_OS::sleep(2);
    resumeTh = new ResumeTransactionThread(suspendTh->getSuspendedTx(), false,
                                           true, &txEvent);
    resumeTh->activate();

    suspendTh->wait();
    delete suspendTh;
    resumeTh->wait();
    ASSERT(!resumeTh->isFailed(), resumeTh->getError());
    delete resumeTh;

    // start suspend thread  and tryresume thread with commit. make tryResume to
    // sleep
    sprintf(buf,
            "start suspend thread  and tryresume thread with commit. make "
            "tryResume to sleep");
    LOG(buf);
    suspendTh = new SuspendTransactionThread(true, &txEvent);
    suspendTh->activate();
    ACE_OS::sleep(2);
    sprintf(buf, "suspendTh->activate();");
    LOG(buf);

    resumeTh = new ResumeTransactionThread(suspendTh->getSuspendedTx(), true,
                                           true, &txEvent);
    resumeTh->activate();

    suspendTh->wait();
    delete suspendTh;
    resumeTh->wait();
    ASSERT(!resumeTh->isFailed(), resumeTh->getError());
    delete resumeTh;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateNonexistentServerRegion_Pooled_Locator)
  {
    initClient(true);
    createPooledRegion("non-region", USE_ACK, locatorsG, "__TESTPOOL1_");
    try {
      createEntry("non-region", keys[0], vals[0]);
      FAIL(
          "Expected exception when doing operations on a non-existent region.");
    } catch (const CacheServerException& ex) {
      printf(
          "Got expected CacheServerException when performing operation "
          "on a non-existent region: %s\n",
          ex.getMessage());
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,
                      CreateNonexistentServerRegion_Pooled_Locator_Sticky)
  {
    initClient(true);
    createPooledRegionSticky("non-region", USE_ACK, locatorsG, "__TESTPOOL1_");
    try {
      createEntry("non-region", keys[0], vals[0]);
      FAIL(
          "Expected exception when doing operations on a non-existent region.");
    } catch (const CacheServerException& ex) {
      printf(
          "Got expected CacheServerException when performing operation "
          "on a non-existent region: %s\n",
          ex.getMessage());
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1PooledRegionWithoutSticky)
  {
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepOne_Pooled complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1PooledRegionWithSticky)
  {
    createPooledRegionSticky(regionNames[0], USE_ACK, locatorsG,
                             "__TESTPOOL1_");
    createPooledRegionSticky(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepOne_Pooled_Locator_Sticky complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2PooledRegionWithoutSticky)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2PooledRegionWithSticky)
  {
    initClient(true);
    createPooledRegionSticky(regionNames[0], USE_ACK, locatorsG,
                             "__TESTPOOL1_");
    createPooledRegionSticky(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1Entries)
  {
    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();
    txManager->begin();
    createEntry(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[1], keys[2], vals[2]);
    txManager->commit();
    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2Entries)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[1], keys[2], vals[2]);
    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();
    txManager->begin();
    createEntry(regionNames[0], keys[1], vals[1]);
    createEntry(regionNames[1], keys[3], vals[3]);
    txManager->commit();
    verifyEntry(regionNames[0], keys[1], vals[1]);
    verifyEntry(regionNames[1], keys[3], vals[3]);
    LOG("StepFour complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, UpdateClient1Entries)
  {
    VectorOfCacheableKey vec0, vec1;
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
    reg0->serverKeys(vec0);
    reg1->serverKeys(vec1);
    ASSERT(vec0.size() == 2, "Should have 2 keys in first region.");
    ASSERT(vec1.size() == 2, "Should have 2 keys in second region.");
    std::string key0, key1;
    key0 = vec0[0]->toString()->asChar();
    key1 = vec0[1]->toString()->asChar();
    ASSERT(key0 != key1, "The two keys should be different in first region.");
    ASSERT(key0 == keys[0] || key0 == keys[1],
           "Unexpected key in first region.");
    ASSERT(key1 == keys[0] || key1 == keys[1],
           "Unexpected key in first region.");

    key0 = vec1[0]->toString()->asChar();
    key1 = vec1[1]->toString()->asChar();
    ASSERT(key0 != key1, "The two keys should be different in second region.");
    ASSERT(key0 == keys[2] || key0 == keys[3],
           "Unexpected key in second region.");
    ASSERT(key1 == keys[2] || key1 == keys[3],
           "Unexpected key in second region.");

    doNetsearch(regionNames[0], keys[1], vals[1]);
    doNetsearch(regionNames[1], keys[3], vals[3]);
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    LOG("StepFive complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, UpdateClient2Entries)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[1], keys[2], vals[2]);
    CacheTransactionManagerPtr txManager =
        getHelper()->getCache()->getCacheTransactionManager();
    txManager->begin();
    updateEntry(regionNames[0], keys[1], nvals[1]);
    updateEntry(regionNames[1], keys[3], nvals[3]);
    txManager->commit();
    LOG("StepSix complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1EntryTwice)
  { createEntryTwice(regionNames[0], CREATE_TWICE_KEY, CREATE_TWICE_VALUE); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1KeyThriceWithoutSticky)
  {
    createPooledRegion(regionNames[2], NO_ACK, locatorsG, "__TESTPOOL1_", false,
                       false);
    RegionPtr reg = getHelper()->getRegion(regionNames[2]);
    LOG("REGION Created with Caching Enabled false");
    CacheableKeyPtr keyPtr = createKey(CREATE_TWICE_KEY);
    CacheableStringPtr valPtr = CacheableString::create(CREATE_TWICE_VALUE);
    try {
      reg->create(keyPtr, valPtr);
      char message[200];
      sprintf(message, "First create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Second create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Third create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
    } catch (const EntryExistsException& gemfireExcp) {
      LOG(gemfireExcp.getMessage());
      ASSERT(false,
             "Creating KEY Twice on a caching-enabled false region should be "
             "allowed.");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1KeyThriceWithSticky)
  {
    createPooledRegionSticky(regionNames[2], NO_ACK, locatorsG, "__TESTPOOL1_",
                             false, false);
    RegionPtr reg = getHelper()->getRegion(regionNames[2]);
    LOG("REGION Created with Caching Enabled false");
    CacheableKeyPtr keyPtr = createKey(CREATE_TWICE_KEY);
    CacheableStringPtr valPtr = CacheableString::create(CREATE_TWICE_VALUE);

    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
    reg0->localInvalidate(createKey(keys[1]));
    reg1->localInvalidate(createKey(keys[3]));
    PoolPtr pool = PoolManager::find("__TESTPOOL1_");
    ASSERT(pool != NULLPTR, "Pool Should have been found");
    doNetsearch(regionNames[0], keys[1], nvals[1]);
    doNetsearch(regionNames[1], keys[3], nvals[3]);
    pool->releaseThreadLocalConnection();
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    pool->releaseThreadLocalConnection();
    try {
      reg->create(keyPtr, valPtr);
      char message[200];
      sprintf(message, "First create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Second create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Third create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
    } catch (const EntryExistsException& gemfireExcp) {
      LOG(gemfireExcp.getMessage());
      ASSERT(false,
             "Creating KEY Twice on a caching-enabled false region should be "
             "allowed.");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

#endif /* THINCLIENTTRANSACTIONS_HPP_ */
