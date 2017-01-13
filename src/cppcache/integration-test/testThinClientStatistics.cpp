/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include <gfcpp/statistics/StatisticsFactory.hpp>

#include <ace/ACE.h>
#include <ace/Guard_T.h>
#include <ace/Thread_Mutex.h>
#include <ace/OS.h>
#include <ace/OS_NS_time.h>
#include <ace/OS_NS_sys_time.h>
#include <ace/OS_NS_unistd.h>
#include <ace/OS_NS_Thread.h>
#include <ace/Dirent.h>
#include <ace/Dirent_Selector.h>
#include <ace/OS_NS_sys_stat.h>

/* This is to test Statistics Functionality, Following Parameters are considered
1-  Creation of Stats Type / Statistics / Statistics Descriptors ( int_t/ Long /
Double ,  Counter / Gauge ) .
2- Use Functions Get/ Set / Inc in normal / abnormal way.  and check consistency
in single / multithreaded env.
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define CLIENT3 s2p2
const char* g_ClientName;
const char* MyClients[] = {"MyClientA", "MyClientB", "MyClientC"};
int LogFileSettings[] = {1, 2, 100};
int MyResults[] = {1 * 1024 * 1024, 2 * 1024 * 1024, 5 * 1024 * 1024};
int NumberOfFiles[] = {10, 2, 1};
int DiskFileSettings[] = {10, 3, 5};

#define EPSILON 0.0000001

#include "locator_globals.hpp"
#include "LocatorHelper.hpp"

bool IsDoubleEqual(double d1, double d2) {
  double diff = d1 > d2 ? (d1 - d2) : (d2 - d1);
  return (diff < EPSILON) ? true : false;
}

struct TestStatisticsType {
  StatisticsType* testStatsType;
  int statIdIntCounter;
  int statIdIntGauge;
  int statIdLongCounter;
  int statIdLongGauge;
  int statIdDoubleCounter;
  int statIdDoubleGauge;
};

class IncThread : public ACE_Task_Base {
 private:
  Statistics* m_stat;
  TestStatisticsType* m_type;

 public:
  IncThread(Statistics* stat, TestStatisticsType* type)
      : m_stat(stat), m_type(type) {}

  int svc(void) {
    /* Just 1000 Inc, Stop after that  */
    for (int incIdx = 0; incIdx < 1000; incIdx++) {
      m_stat->incInt(m_type->statIdIntCounter, 1);
      m_stat->incInt(m_type->statIdIntGauge, 1);
      m_stat->incLong(m_type->statIdLongCounter, 1);
      m_stat->incLong(m_type->statIdLongGauge, 1);
      m_stat->incDouble(m_type->statIdDoubleCounter, 1.0);
      m_stat->incDouble(m_type->statIdDoubleGauge, 1.0);
    }
    // LOG(" Incremented 1000 times by thread.");
    return 0;
  }
  void start() { activate(); }
  void stop() { wait(); }
};

static int comparator(const dirent** d1, const dirent** d2) {
  if (strlen((*d1)->d_name) < strlen((*d2)->d_name)) {
    return -1;
  } else if (strlen((*d1)->d_name) > strlen((*d2)->d_name)) {
    return 1;
  }

  int diff = ACE_OS::strcmp((*d1)->d_name, (*d2)->d_name);
  if (diff < 0) {
    return -1;
  } else if (diff > 0) {
    return 1;
  } else {
    return 0;
  }
}

static int selector(const dirent* d) {
  std::string inputname(d->d_name);
  LOGINFO("selector %s\t g_ClientName = %s", inputname.c_str(), g_ClientName);
  return ACE_OS::strstr(d->d_name, g_ClientName) != 0;
}

/* Common Functions */
void initClientWithStats() {
  PropertiesPtr pp = Properties::create();
  pp->insert("statistic-sampling-enabled", "true");
  pp->insert("statistic-sample-rate", 1);
  pp->insert("statistic-archive-file", "./statArchive.gfs");
  pp->insert("notify-ack-interval", 1);

  initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1", pp, 0,
                     true);
  getHelper()->createPooledRegion(regionNames[0], USE_ACK, locatorsG,
                                  "__TEST_POOL1__", true, true);
}

void initClientWithStatsDisabled() {
  PropertiesPtr pp = Properties::create();
  pp->insert("statistic-sampling-enabled", "false");
  // pp->insert("statistic-sample-rate", 1);
  // pp->insert("statistic-archive-file", "./statArchive.gfs");

  initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1", pp, 0,
                     true);
  getHelper()->createPooledRegion(regionNames[0], USE_ACK, locatorsG,
                                  "__TEST_POOL1__", true, true);
}

void DoRegionOpsAndVerify() {
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

  for (int index = 0; index < 5; index++) {
    char key[100] = {0};
    char value[100] = {0};
    ACE_OS::sprintf(key, "Key-%d", index);
    ACE_OS::sprintf(value, "Value-%d", index);
    CacheableKeyPtr keyptr = CacheableKey::create(key);
    CacheablePtr valuePtr = CacheableString::create(value);
    regPtr0->put(keyptr, valuePtr);
  }

  CacheableKeyPtr keyptr = CacheableKey::create("Key-0");

  RegionEntryPtr regEntry = regPtr0->getEntry(keyptr);

  LOGINFO("regEntry->isDestroyed() = %d ", regEntry->isDestroyed());
  ASSERT(
      regEntry->isDestroyed() == false,
      "regionEntry is not destroyed, regEntry->isDestroyed must return false");

  bool flag ATTR_UNUSED = regPtr0->remove(keyptr);

  RegionEntryPtr remRegEntry = regPtr0->getEntry(keyptr);
  ASSERT(remRegEntry == NULLPTR,
         "regionEntry pointer to removed entry must be NULLPTR");

  if (remRegEntry != NULLPTR) {
    LOGINFO("remRegEntry->isDestroyed() = %d ", remRegEntry->isDestroyed());
    ASSERT(remRegEntry->isDestroyed() == true,
           "regionEntry is not destroyed, remRegEntry->isDestroyed must return "
           "false");
  } else {
    LOGINFO("regionEntry pointer for removed key is NULL");
  }

  CacheStatisticsPtr cacheStatptr(new CacheStatistics());
  // CacheStatisticsPtr cacheStatptr;
  try {
    CachePtr cache = dynCast<CachePtr>(
        regPtr0->getRegionService());  // This depends on LocalCache
                                       // implementing RegionService...
    bool flag = cache->getDistributedSystem()
                    ->getSystemProperties()
                    ->statisticsEnabled();
    LOGINFO("statisticsEnabled = %d ", flag);
    regEntry->getStatistics(cacheStatptr);
  } catch (StatisticsDisabledException& ex) {
    LOGINFO("Exception Caught:: StatisticsDisabledException");
  } catch (GemfireConfigException& e) {
    LOGINFO("Exception Caught:: %s", e.getMessage());
  } catch (Exception& ex) {
    LOGINFO("Exception Caught:: %s", ex.getMessage());
  }
  if (cacheStatptr != NULLPTR) {
    LOGINFO("LastAccessedTime = %d ", cacheStatptr->getLastAccessedTime());
  } else {
    LOGINFO("cacheStatptr is NULL");
  }
}

void initClientWithStatsAndLog(const char* str, int fileLimit, int diskLimit) {
  PropertiesPtr pp = Properties::create();
  pp->insert("log-file", str);
  pp->insert("log-level", "fine");
  pp->insert("log-file-size-limit", fileLimit);
  pp->insert("log-disk-space-limit", diskLimit);
  initClient(true, pp);
}

void createType(StatisticsFactory* statFactory, TestStatisticsType& testType) {
  StatisticDescriptor** statDescriptorArr = new StatisticDescriptor*[6];
  statDescriptorArr[0] = statFactory->createIntCounter(
      "IntCounter", "Test Statistic Descriptor int_t Counter.", "TestUnit");

  statDescriptorArr[1] = statFactory->createIntGauge(
      "IntGauge", "Test Statistic Descriptor int_t Gauge.", "TestUnit");

  statDescriptorArr[2] = statFactory->createLongCounter(
      "LongCounter", "Test Statistic Descriptor Long Counter.", "TestUnit");

  statDescriptorArr[3] = statFactory->createLongGauge(
      "LongGauge", "Test Statistic Descriptor Long Gauge.", "TestUnit");

  statDescriptorArr[4] = statFactory->createDoubleCounter(
      "DoubleCounter", "Test Statistic Descriptor Double Counter.", "TestUnit");

  statDescriptorArr[5] = statFactory->createDoubleGauge(
      "DoubleGauge", "Test Statistic Descriptor Double Gauge.", "TestUnit");

  StatisticsType* statsType = statFactory->createType(
      "TestStatsType", "Statistics for Unit Test.", statDescriptorArr, 6);

  ASSERT(statsType != NULL, "Error in creating Stats Type");

  testType.testStatsType = statsType;
  testType.statIdIntCounter = statsType->nameToId("IntCounter");
  testType.statIdIntGauge = statsType->nameToId("IntGauge");
  testType.statIdLongCounter = statsType->nameToId("LongCounter");
  testType.statIdLongGauge = statsType->nameToId("LongGauge");
  testType.statIdDoubleCounter = statsType->nameToId("DoubleCounter");
  testType.statIdDoubleGauge = statsType->nameToId("DoubleGauge");

  /* Test Find */
  ASSERT(statsType == statFactory->findType("TestStatsType"),
         " Find Type Failed");
}

void testGetSetIncFunctions(Statistics* stat, TestStatisticsType& type) {
  /* Set a initial value =  10 */
  stat->setInt(type.statIdIntCounter, 10);
  stat->setInt(type.statIdIntGauge, 10);
  stat->setLong(type.statIdLongCounter, 10);
  stat->setLong(type.statIdLongGauge, 10);
  stat->setDouble(type.statIdDoubleCounter, 10.0);
  stat->setDouble(type.statIdDoubleGauge, 10.0);
  LOG(" Setting Initial Value Complete");

  /* Check Initial Value = 10*/
  ASSERT(10 == stat->getInt(type.statIdIntCounter), " Check1 1 Failed ");
  ASSERT(10 == stat->getInt(type.statIdIntGauge), " Check1 2 Failed ");
  ASSERT(10 == stat->getLong(type.statIdLongCounter), " Check1 3 Failed ");
  ASSERT(10 == stat->getLong(type.statIdLongGauge), " Check1 4 Failed ");
  ASSERT(IsDoubleEqual(10.0, stat->getDouble(type.statIdDoubleCounter)),
         " Check1 5 Failed ");
  ASSERT(IsDoubleEqual(10.0, stat->getDouble(type.statIdDoubleGauge)),
         " Check1 6 Failed ");
  LOG(" All Set() were correct.");

  /* Increment single thread for 100 times */
  for (int incIdx = 0; incIdx < 100; incIdx++) {
    stat->incInt(type.statIdIntCounter, 1);
    stat->incInt(type.statIdIntGauge, 1);
    stat->incLong(type.statIdLongCounter, 1);
    stat->incLong(type.statIdLongGauge, 1);
    stat->incDouble(type.statIdDoubleCounter, 1.0);
    stat->incDouble(type.statIdDoubleGauge, 1.0);
    SLEEP(10);
  }
  LOG(" Incremented 100 times by 1.");

  /* Check Incremented Value = 110 */
  ASSERT(110 == stat->getInt(type.statIdIntCounter), " Check2 1 Failed ");
  ASSERT(110 == stat->getInt(type.statIdIntGauge), " Check2 2 Failed ");
  ASSERT(110 == stat->getLong(type.statIdLongCounter), " Check2 3 Failed ");
  ASSERT(110 == stat->getLong(type.statIdLongGauge), " Check2 4 Failed ");
  ASSERT(IsDoubleEqual(110.0, stat->getDouble(type.statIdDoubleCounter)),
         " Check2 5 Failed ");
  ASSERT(IsDoubleEqual(110.0, stat->getDouble(type.statIdDoubleGauge)),
         " Check2 6 Failed ");
  LOG(" Single thread Inc() Passed.");

  /* Increment parallelly  = 10,000 times */
  IncThread* threads[10];
  for (int thdIdx = 0; thdIdx < 10; thdIdx++) {
    threads[thdIdx] = new IncThread(stat, &type);
    threads[thdIdx]->start();
  }
  SLEEP(1000);
  for (int thdIdx = 0; thdIdx < 10; thdIdx++) {
    threads[thdIdx]->stop();
  }

  /* Check Final Value = 10,110 */
  ASSERT(10110 == stat->getInt(type.statIdIntCounter), " Check2 1 Failed ");
  ASSERT(10110 == stat->getInt(type.statIdIntGauge), " Check2 2 Failed ");
  ASSERT(10110 == stat->getLong(type.statIdLongCounter), " Check2 3 Failed ");
  ASSERT(10110 == stat->getLong(type.statIdLongGauge), " Check2 4 Failed ");
  ASSERT(IsDoubleEqual(10110.0, stat->getDouble(type.statIdDoubleCounter)),
         " Check2 5 Failed ");
  ASSERT(IsDoubleEqual(10110.0, stat->getDouble(type.statIdDoubleGauge)),
         " Check2 6 Failed ");
  LOG(" Parallel Inc() Passed.");
}

void statisticsTest() {
  /* Create Statistics in right and wrong manner */
  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();

  /* Register a type */
  TestStatisticsType testType;
  createType(factory, testType);
  LOG("Statistics Type TestStats Registered");

  /* Create a statistics */
  Statistics* testStat1 =
      factory->createStatistics(testType.testStatsType, "TestStatistics");
  ASSERT(testStat1 != NULL, "Test Statistics Creation Failed");

  /* Tests Find Type , Find Statistics */
  Statistics* temp = factory->findFirstStatisticsByType(testType.testStatsType);
  ASSERT(temp == testStat1, "findFirstStatisticsByType Failed");
  LOG("Statistics testStat1 Created Successfully.");

  /* Test Set Functions */
  testGetSetIncFunctions(testStat1, testType);
  LOG("Get / Set / Inc Functions Tested ");

  /* Close Statistics */
  testStat1->close();
  Statistics* temp2 =
      factory->findFirstStatisticsByType(testType.testStatsType);
  ASSERT(temp2 == NULL, "Statistics close() Failed");

  LOG("StatisticsTest Completed");
}

void LogTest(int expectedResult, int noofFiles, int diskLimit) {
  unsigned int seed = 2;
  int rand1 = ACE_OS::rand_r(&seed) % 2;
  unsigned int seed2 = 700;
  int rand2 = ACE_OS::rand_r(&seed2) % 700;

  int rand = (rand1 + 8) * 1024 + rand2;
  LOGFINE(" random is %d", rand);
  char buf[10 * 1024];
  for (int i = 0; i < rand - 1; i++) {
    buf[i] = 'A';
  }
  buf[10 * 1024 - 1] = '\0';
  for (int j = 0; j < 2 * 1024; j++) {
    LOGFINE("%s", buf);
  }
  std::string dirname = ACE::dirname(".");
  struct dirent** resultArray;
  LOGFINE("dirname is %s ", dirname.c_str());
  int entries_count =
      ACE_OS::scandir(dirname.c_str(), &resultArray, selector, comparator);
  LOGFINE("entries_count is %d ", entries_count);
  ASSERT(entries_count == noofFiles,
         "files count should be equal to noofFiles");
  ACE_stat statBuf;
  int64_t spaceUsed = 0;
  char fullpath[512] = {0};
  for (int i = 0; i < entries_count; i++) {
    sprintf(fullpath, "%s%c%s", dirname.c_str(), ACE_DIRECTORY_SEPARATOR_CHAR,
            resultArray[i]->d_name);
    LOGFINE("fullpath is %s ", fullpath);
    ACE_OS::stat(fullpath, &statBuf);
    int64_t size = statBuf.st_size;
    ASSERT(size <= expectedResult,
           "individual file size should be less than equal to expectedResult");
    spaceUsed += size;
  }
  LOGFINE("spaceUsed is %d ", spaceUsed);
  ASSERT(spaceUsed <= 1024 * 1024 * diskLimit,
         "collective file size should be less than equal to diskLimit");
  LOG("LogTest Completed");
}

void StatFileTest() {
  LOG(" Starting Stat.gfs file test");
  /* Get Pid , Get File name : HARD Coding for File name pattern */
  char buff[1024];
  int32 pid = ACE_OS::getpid();
  ACE_OS::sprintf(buff, "./statArchive-%d.gfs", pid);
  std::string statFilename(buff);

  /* Test if this file Stat-pid.gfs is there */
  FILE* fp = fopen(statFilename.c_str(), "r");
  ASSERT(fp != NULL, "Statistics GFS file does not exist");
  if (fp != NULL) {
    LOG("SUCCESS: .gfs file exist.");
    fclose(fp);
  }

  LOG(" StatFileTest complete.");
}

/* Test Tasks */
DUNIT_TASK_DEFINITION(SERVER1, StartFirstServer)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ClientFirstInit)
  {
    initClientWithStats();
    LOG("Client Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StatTest)
  { statisticsTest(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseFirstClient)
  {
    cleanProc();
    LOG("CLIENT closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, GFSFileTest)
  { StatFileTest(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseFirstServer)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartSecondServer)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ClientSecondInit)
  {
    g_ClientName = MyClients[0];
    initClientWithStatsAndLog(MyClients[0], LogFileSettings[0],
                              DiskFileSettings[0]);
    LogTest(MyResults[0], NumberOfFiles[0], DiskFileSettings[0]);
    cleanProc();
    g_ClientName = MyClients[1];
    initClientWithStatsAndLog(MyClients[1], LogFileSettings[1],
                              DiskFileSettings[1]);
    LogTest(MyResults[1], NumberOfFiles[1], DiskFileSettings[1]);
    cleanProc();
    g_ClientName = MyClients[2];
    initClientWithStatsAndLog(MyClients[2], LogFileSettings[2],
                              DiskFileSettings[2]);
    LogTest(MyResults[2], NumberOfFiles[2], DiskFileSettings[2]);
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseSecondServer)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

// ADDED FOR TEST-COVERAGE
DUNIT_TASK_DEFINITION(SERVER1, StartThirdServer)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ClientThirdInit)
  {
    initClientWithStatsDisabled();
    LOG("Client Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, RegionOps)
  { DoRegionOpsAndVerify(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseThirdClient)
  {
    cleanProc();
    LOG("CLIENT closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseThirdServer)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1)

    CALL_TASK(StartFirstServer)
    CALL_TASK(ClientFirstInit)
    CALL_TASK(StatTest)
    CALL_TASK(CloseFirstClient)
    CALL_TASK(GFSFileTest)
    CALL_TASK(CloseFirstServer)
    CALL_TASK(StartSecondServer)
    CALL_TASK(ClientSecondInit)
    CALL_TASK(CloseSecondServer)
    CALL_TASK(StartThirdServer)
    CALL_TASK(ClientThirdInit)
    CALL_TASK(RegionOps)
    CALL_TASK(CloseThirdClient)
    CALL_TASK(CloseThirdServer)

    CALL_TASK(CloseLocator1)
  }
END_MAIN
