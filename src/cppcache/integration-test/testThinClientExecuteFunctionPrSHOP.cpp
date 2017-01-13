/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientExecuteFunctionPrSHOP"

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "testobject/VariousPdxTypes.hpp"

using namespace PdxTests;
/* This is to test
1- funtion execution on pool
*/

#define CLIENT1 s1p1
#define LOCATOR1 s2p1
#define SERVER s2p2

bool isLocalServer = false;
bool isLocator = false;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* poolRegNames[] = {"partition_region", "PoolRegion2"};

const char* serverGroup = "ServerGroup1";

char* FEOnRegionPrSHOP = (char*)"FEOnRegionPrSHOP";
char* FEOnRegionPrSHOP_OptimizeForWrite =
    (char*)"FEOnRegionPrSHOP_OptimizeForWrite";
char* getFuncName = (char*)"MultiGetFunction";
char* putFuncName = (char*)"MultiPutFunction";
char* putFuncIName = (char*)"MultiPutFunctionI";
char* FETimeOut = (char*)"FunctionExecutionTimeOut";

class MyResultCollector : public ResultCollector {
 public:
  MyResultCollector()
      : m_resultList(CacheableVector::create()),
        m_isResultReady(false),
        m_endResultCount(0),
        m_addResultCount(0),
        m_getResultCount(0) {}
  ~MyResultCollector() {}
  CacheableVectorPtr getResult(uint32_t timeout) {
    m_getResultCount++;
    if (m_isResultReady == true) {
      return m_resultList;
    } else {
      for (uint32_t i = 0; i < timeout; i++) {
        SLEEP(1);
        if (m_isResultReady == true) return m_resultList;
      }
      throw FunctionExecutionException(
          "Result is not ready, endResults callback is called before invoking "
          "getResult() method");
    }
  }

  void addResult(CacheablePtr& resultItem) {
    m_addResultCount++;
    if (resultItem == NULLPTR) {
      return;
    }
    try {
      CacheableArrayListPtr result = dynCast<CacheableArrayListPtr>(resultItem);
      for (int32_t i = 0; i < result->size(); i++) {
        m_resultList->push_back(result->operator[](i));
      }
    } catch (ClassCastException) {
      UserFunctionExecutionExceptionPtr result =
          dynCast<UserFunctionExecutionExceptionPtr>(resultItem);
      m_resultList->push_back(result);
    }
  }
  void endResults() {
    m_isResultReady = true;
    m_endResultCount++;
  }
  uint32_t getEndResultCount() { return m_endResultCount; }
  uint32_t getAddResultCount() { return m_addResultCount; }
  uint32_t getGetResultCount() { return m_getResultCount; }

 private:
  CacheableVectorPtr m_resultList;
  volatile bool m_isResultReady;
  uint32_t m_endResultCount;
  uint32_t m_addResultCount;
  uint32_t m_getResultCount;
};
typedef SharedPtr<MyResultCollector> MyResultCollectorPtr;
class MyResultCollector2 : public ResultCollector {
 public:
  MyResultCollector2()
      : m_resultList(CacheableVector::create()),
        m_isResultReady(false),
        m_endResultCount(0),
        m_addResultCount(0),
        m_getResultCount(0) {}
  ~MyResultCollector2() {}
  CacheableVectorPtr getResult(uint32_t timeout) {
    m_getResultCount++;
    if (m_isResultReady == true) {
      return m_resultList;
    } else {
      for (uint32_t i = 0; i < timeout; i++) {
        SLEEP(1);
        if (m_isResultReady == true) return m_resultList;
      }
      throw FunctionExecutionException(
          "Result is not ready, endResults callback is called before invoking "
          "getResult() method");
    }
  }

  void addResult(CacheablePtr& resultItem) {
    m_addResultCount++;
    if (resultItem == NULLPTR) {
      return;
    }
    CacheableBooleanPtr result = dynCast<CacheableBooleanPtr>(resultItem);
    m_resultList->push_back(result);
  }
  void endResults() {
    m_isResultReady = true;
    m_endResultCount++;
  }
  uint32_t getEndResultCount() { return m_endResultCount; }
  uint32_t getAddResultCount() { return m_addResultCount; }
  uint32_t getGetResultCount() { return m_getResultCount; }

 private:
  CacheableVectorPtr m_resultList;
  volatile bool m_isResultReady;
  uint32_t m_endResultCount;
  uint32_t m_addResultCount;
  uint32_t m_getResultCount;
};
typedef SharedPtr<MyResultCollector2> MyResultCollectorPtr2;

DUNIT_TASK_DEFINITION(LOCATOR1, StartLocator1)
  {
    // starting locator
    if (isLocator) {
      CacheHelper::initLocator(1);
      LOG("Locator1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, StartS13)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "func_cacheserver1_pool.xml", locHostPort);
    }
    if (isLocalServer) {
      CacheHelper::initServer(2, "func_cacheserver2_pool.xml", locHostPort);
    }
    if (isLocalServer) {
      CacheHelper::initServer(3, "func_cacheserver3_pool.xml", locHostPort);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StartC1)
  {
    initClientWithPool(true, NULL, locHostPort, serverGroup, NULLPTR, 0, true,
                       -1, -1, 60000, /*singlehop*/ true,
                       /*threadLocal*/ true);

    RegionPtr regPtr0 =
        createRegionAndAttachPool(poolRegNames[0], USE_ACK, NULL);
    ;
    regPtr0->registerAllKeys();

    LOG("Clnt1Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1OpTest2)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(poolRegNames[0]);
    char buf[128];

    for (int i = 0; i < 230; i++) {
      sprintf(buf, "VALUE--%d", i);
      CacheablePtr value(CacheableString::create(buf));
      regPtr0->put(i, value);
    }
    LOG("Put done.");
    try {
      bool getResult = true;
      for (int k = 0; k < 210; k++) {
        CacheableVectorPtr routingObj = CacheableVector::create();
        for (int i = k; i < k + 20; i++) {
          routingObj->push_back(CacheableInt32::create(i));
        }
        LOGINFO("routingObj size = %d ", routingObj->size());
        ExecutionPtr exe = FunctionService::onRegion(regPtr0);
        ASSERT(exe != NULLPTR, "onRegion Returned NULL");

        CacheableVectorPtr resultList = CacheableVector::create();
        LOG("Executing getFuncName function");
        CacheableVectorPtr executeFunctionResult =
            exe->withFilter(routingObj)->execute(getFuncName, 15)->getResult();
        if (executeFunctionResult == NULLPTR) {
          ASSERT(false, "executeFunctionResult is NULL");
        } else {
          sprintf(buf, "result count = %d", executeFunctionResult->size());
          LOGINFO(buf);
          resultList->clear();
          for (unsigned item = 0;
               item < static_cast<uint32_t>(executeFunctionResult->size());
               item++) {
            CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
                executeFunctionResult->operator[](item));
            for (unsigned pos = 0;
                 pos < static_cast<uint32_t>(arrayList->size()); pos++) {
              resultList->push_back(arrayList->operator[](pos));
            }
          }
          sprintf(buf, "get result count = %d", resultList->size());
          LOGINFO(buf);
          ASSERT(resultList->size() == 40,
                 "get executeFunctionResult count is not 40");
          for (int32_t i = 0; i < resultList->size(); i++) {
            sprintf(buf, "result[%d] is null\n", i);
            ASSERT(resultList->operator[](i) != NULLPTR, buf);
            sprintf(buf, "get result[%d]=%s", i,
                    dynCast<CacheableStringPtr>(resultList->operator[](i))
                        ->asChar());
            LOGINFO(buf);
          }
        }
        LOGINFO("getFuncName done");
        MyResultCollectorPtr myRC(new MyResultCollector());
        CacheableVectorPtr executeFunctionResult1 =
            exe->withFilter(routingObj)
                ->withCollector(myRC)
                ->execute(getFuncName, getResult)
                ->getResult();
        LOGINFO("add result count = %d", myRC->getAddResultCount());
        LOGINFO("end result count = %d", myRC->getEndResultCount());
        LOGINFO("get result count = %d", myRC->getGetResultCount());
        ASSERT(4 == myRC->getAddResultCount(), "add result count is not 4");
        ASSERT(1 == myRC->getEndResultCount(), "end result count is not 1");
        ASSERT(1 == myRC->getGetResultCount(), "get result count is not 1");
        if (executeFunctionResult == NULLPTR) {
          ASSERT(false, "region get new collector: result is NULL");
        } else {
          sprintf(buf, "result count = %d", executeFunctionResult->size());
          LOGINFO(buf);
          resultList->clear();
          for (unsigned item = 0;
               item < static_cast<uint32_t>(executeFunctionResult->size());
               item++) {
            CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
                executeFunctionResult->operator[](item));
            for (unsigned pos = 0;
                 pos < static_cast<uint32_t>(arrayList->size()); pos++) {
              resultList->push_back(arrayList->operator[](pos));
            }
          }
          sprintf(buf, "get result count = %d", resultList->size());
          LOGINFO(buf);
          ASSERT(resultList->size() == 40,
                 "get executeFunctionResult count is not 40");
          for (int32_t i = 0; i < resultList->size(); i++) {
            sprintf(buf, "result[%d] is null\n", i);
            ASSERT(resultList->operator[](i) != NULLPTR, buf);
            sprintf(buf, "get result[%d]=%s", i,
                    dynCast<CacheableStringPtr>(resultList->operator[](i))
                        ->asChar());
            LOGINFO(buf);
          }
        }
        LOGINFO("getFuncName MyResultCollector done");

        CacheableVectorPtr executeFunctionResult2 =
            exe->withFilter(routingObj)->execute(FEOnRegionPrSHOP)->getResult();
        if (executeFunctionResult2 == NULLPTR) {
          ASSERT(false, "executeFunctionResult2 is NULL");
        } else {
          sprintf(buf, "result count = %d", executeFunctionResult2->size());
          LOG(buf);
          ASSERT(2 == executeFunctionResult2->size(),
                 "executeFunctionResult2 size is not 2");
          for (int i = 0; i < executeFunctionResult2->size(); i++) {
            bool b = dynCast<CacheableBooleanPtr>(
                         executeFunctionResult2->operator[](i))
                         ->value();
            LOG(b == true ? "true" : "false");
            ASSERT(b == true, "true is not eched back");
          }
        }
        LOGINFO("FEOnRegionPrSHOP withFilter done");

        ///////////////////////// Now same with ResultCollector
        ////////////////////////////

        MyResultCollectorPtr2 myRC2(new MyResultCollector2());
        CacheableVectorPtr executeFunctionResult21 =
            exe->withFilter(routingObj)
                ->withCollector(myRC2)
                ->execute(FEOnRegionPrSHOP)
                ->getResult();
        LOGINFO("add result count = %d", myRC2->getAddResultCount());
        LOGINFO("end result count = %d", myRC2->getEndResultCount());
        LOGINFO("get result count = %d", myRC2->getGetResultCount());
        ASSERT(2 == myRC2->getAddResultCount(), "add result count is not 2");
        ASSERT(1 == myRC2->getEndResultCount(), "end result count is not 1");
        ASSERT(1 == myRC2->getGetResultCount(), "get result count is not 1");
        if (executeFunctionResult21 == NULLPTR) {
          ASSERT(false, "executeFunctionResult21 is NULL");
        } else {
          sprintf(buf, "result count = %d", executeFunctionResult21->size());
          LOG(buf);
          ASSERT(2 == executeFunctionResult21->size(),
                 "executeFunctionResult21 size is not 2");
          for (int i = 0; i < executeFunctionResult21->size(); i++) {
            bool b = dynCast<CacheableBooleanPtr>(
                         executeFunctionResult21->operator[](i))
                         ->value();
            LOG(b == true ? "true" : "false");
            ASSERT(b == true, "true is not eched back");
          }
        }
        LOGINFO("FEOnRegionPrSHOP done with ResultCollector withFilter");

        /////////////////////// Done with ResultCollector
        ////////////////////////////////

        CacheableVectorPtr executeFunctionResult3 =
            exe->withFilter(routingObj)
                ->execute(FEOnRegionPrSHOP_OptimizeForWrite)
                ->getResult();
        if (executeFunctionResult3 == NULLPTR) {
          ASSERT(false, "executeFunctionResult3 is NULL");
        } else {
          sprintf(buf, "result count = %d", executeFunctionResult3->size());
          LOG(buf);
          ASSERT(3 == executeFunctionResult3->size(),
                 "executeFunctionResult3->size() is not 3");
          for (int i = 0; i < executeFunctionResult3->size(); i++) {
            bool b = dynCast<CacheableBooleanPtr>(
                         executeFunctionResult3->operator[](i))
                         ->value();
            LOG(b == true ? "true" : "false");
            ASSERT(b == true, "true is not eched back");
          }
        }
        LOGINFO("FEOnRegionPrSHOP_OptimizeForWrite withFilter done");

        ///////////////////////// Now same with ResultCollector
        ////////////////////////////
        MyResultCollectorPtr2 myRC3(new MyResultCollector2());
        CacheableVectorPtr executeFunctionResult31 =
            exe->withFilter(routingObj)
                ->withCollector(myRC3)
                ->execute(FEOnRegionPrSHOP_OptimizeForWrite)
                ->getResult();
        LOGINFO("add result count = %d", myRC3->getAddResultCount());
        LOGINFO("end result count = %d", myRC3->getEndResultCount());
        LOGINFO("get result count = %d", myRC3->getGetResultCount());
        ASSERT(3 == myRC3->getAddResultCount(), "add result count is not 3");
        ASSERT(1 == myRC3->getEndResultCount(), "end result count is not 1");
        ASSERT(1 == myRC3->getGetResultCount(), "get result count is not 1");
        if (executeFunctionResult31 == NULLPTR) {
          ASSERT(false, "executeFunctionResult31 is NULL");
        } else {
          sprintf(buf, "result count = %d", executeFunctionResult31->size());
          LOG(buf);
          ASSERT(3 == executeFunctionResult31->size(),
                 "executeFunctionResult31->size() is not 3");
          for (int i = 0; i < executeFunctionResult31->size(); i++) {
            bool b = dynCast<CacheableBooleanPtr>(
                         executeFunctionResult31->operator[](i))
                         ->value();
            LOG(b == true ? "true" : "false");
            ASSERT(b == true, "true is not eched back");
          }
        }
        LOGINFO(
            "FEOnRegionPrSHOP_OptimizeForWrite done with ResultCollector "
            "withFilter");
      }

      ExecutionPtr exc = FunctionService::onRegion(regPtr0);
      ASSERT(exc != NULLPTR, "onRegion Returned NULL");
      // Now w/o filter, chk for singlehop
      CacheableVectorPtr executeFunctionResult2 =
          exc->execute(FEOnRegionPrSHOP)->getResult();
      if (executeFunctionResult2 == NULLPTR) {
        ASSERT(false, "executeFunctionResult2 is NULL");
      } else {
        sprintf(buf, "result count = %d", executeFunctionResult2->size());
        LOG(buf);
        ASSERT(2 == executeFunctionResult2->size(),
               "executeFunctionResult2 size is not 2");
        for (int i = 0; i < executeFunctionResult2->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(
                       executeFunctionResult2->operator[](i))
                       ->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      executeFunctionResult2->clear();
      LOGINFO("FEOnRegionPrSHOP without Filter done");

      // Now w/o filter chk single hop
      MyResultCollectorPtr2 resultCollector(new MyResultCollector2());
      CacheableVectorPtr executeFunctionResult21 =
          exc->withCollector(resultCollector)
              ->execute(FEOnRegionPrSHOP)
              ->getResult();
      LOGINFO("add result count = %d", resultCollector->getAddResultCount());
      LOGINFO("end result count = %d", resultCollector->getEndResultCount());
      LOGINFO("get result count = %d", resultCollector->getGetResultCount());
      ASSERT(2 == resultCollector->getAddResultCount(),
             "add result count is not 2");
      ASSERT(1 == resultCollector->getEndResultCount(),
             "end result count is not 1");
      ASSERT(1 == resultCollector->getGetResultCount(),
             "get result count is not 1");
      if (executeFunctionResult21 == NULLPTR) {
        ASSERT(false, "executeFunctionResult21 is NULL");
      } else {
        sprintf(buf, "result count = %d", executeFunctionResult21->size());
        LOG(buf);
        ASSERT(2 == executeFunctionResult21->size(),
               "executeFunctionResult21 size is not 2");
        for (int i = 0; i < executeFunctionResult21->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(
                       executeFunctionResult21->operator[](i))
                       ->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      LOGINFO("FEOnRegionPrSHOP done with ResultCollector without filter");

      // Now w/o filter chk for singleHop
      MyResultCollectorPtr2 rC(new MyResultCollector2());
      CacheableVectorPtr executeFunctionResult31 =
          exc->withCollector(rC)
              ->execute(FEOnRegionPrSHOP_OptimizeForWrite)
              ->getResult();
      LOGINFO("add result count = %d", rC->getAddResultCount());
      LOGINFO("end result count = %d", rC->getEndResultCount());
      LOGINFO("get result count = %d", rC->getGetResultCount());
      ASSERT(3 == rC->getAddResultCount(), "add result count is not 3");
      ASSERT(1 == rC->getEndResultCount(), "end result count is not 1");
      ASSERT(1 == rC->getGetResultCount(), "get result count is not 1");
      if (executeFunctionResult31 == NULLPTR) {
        ASSERT(false, "executeFunctionResult31 is NULL");
      } else {
        sprintf(buf, "result count = %d", executeFunctionResult31->size());
        LOG(buf);
        ASSERT(3 == executeFunctionResult31->size(),
               "executeFunctionResult31->size() is not 3");
        for (int i = 0; i < executeFunctionResult31->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(
                       executeFunctionResult31->operator[](i))
                       ->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      LOGINFO(
          "FEOnRegionPrSHOP_OptimizeForWrite done with ResultCollector without "
          "Filter.");

      // Now w/o filter chk for singleHop
      CacheableVectorPtr functionResult =
          exc->execute(FEOnRegionPrSHOP_OptimizeForWrite)->getResult();
      if (functionResult == NULLPTR) {
        ASSERT(false, "functionResult is NULL");
      } else {
        sprintf(buf, "result count = %d", functionResult->size());
        LOG(buf);
        ASSERT(3 == functionResult->size(), "FunctionResult->size() is not 3");
        for (int i = 0; i < functionResult->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(functionResult->operator[](i))
                       ->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      LOGINFO("FEOnRegionPrSHOP_OptimizeForWrite without Filter done");
      /////////////////////// Done with ResultCollector
      ////////////////////////////////

      char KeyStr[256] = {0};
      char valStr[256] = {0};
      CacheableVectorPtr fil = CacheableVector::create();
      for (int i = 0; i < 500; i++) {
        ACE_OS::snprintf(KeyStr, 256, "KEY--%d ", i);
        ACE_OS::snprintf(valStr, 256, "VALUE--%d ", i);
        CacheableStringPtr keyport = CacheableString::create(KeyStr);
        CacheablePtr valport = CacheableString::create(valStr);
        regPtr0->put(keyport, valport);
        fil->push_back(CacheableString::create(KeyStr));
      }
      LOGINFO("Put on region complete ");
      LOGINFO("filter count= {0}.", fil->size());

      // Fire N Forget with filter keys
      exc->withFilter(fil)->execute(putFuncName);
      SLEEP(4000);
      LOGINFO(
          "Executing ExecuteFunctionOnRegion on region for execKeys for "
          "arrList "
          "arguement done.");
      for (int i = 0; i < fil->size(); i++) {
        CacheableStringPtr str = fil->at(i);
        CacheableStringPtr val = dynCast<CacheableStringPtr>(regPtr0->get(str));
        LOGINFO("Filter Key = %s , get Value = %s ", str->asChar(),
                val->asChar());
        if (strcmp(str->asChar(), val->asChar()) != 0) {
          ASSERT(false, "Value after function execution is incorrect");
        }
      }

      // Fire N Forget without filter keys
      CacheableArrayListPtr arrList = CacheableArrayList::create();
      for (int i = 10; i < 200; i++) {
        ACE_OS::snprintf(KeyStr, 256, "KEY--%d ", i);
        arrList->push_back(CacheableString::create(KeyStr));
      }
      ExecutionPtr ex = FunctionService::onRegion(regPtr0);
      ex->withArgs(arrList)->execute(putFuncIName);
      LOGINFO(
          "Executing ExecuteFunctionOnRegion on region for execKeys for "
          "arrList "
          "arguement done.");
      SLEEP(4000);
      for (int i = 0; i < arrList->size(); i++) {
        CacheableStringPtr str = arrList->at(i);
        CacheableStringPtr val = dynCast<CacheableStringPtr>(regPtr0->get(str));
        LOGINFO("Filter Key = %s ", str->asChar());
        LOGINFO("get Value = %s ", val->asChar());
        if (strcmp(str->asChar(), val->asChar()) != 0) {
          ASSERT(false, "Value after function execution is incorrect");
        }
      }

      ///////////////////TimeOut test //////////////////////
      LOGINFO("FETimeOut begin onRegion");
      ExecutionPtr RexecutionPtr = FunctionService::onRegion(regPtr0);
      CacheableVectorPtr fe =
          RexecutionPtr->withArgs(CacheableInt32::create(5000))
              ->execute(FETimeOut, 5000)
              ->getResult();
      if (fe == NULLPTR) {
        ASSERT(false, "functionResult is NULL");
      } else {
        sprintf(buf, "result count = %d", fe->size());
        LOG(buf);
        ASSERT(2 == fe->size(), "FunctionResult->size() is not 2");
        for (int i = 0; i < fe->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(fe->operator[](i))->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      LOGINFO("FETimeOut done onRegion");
    } catch (const Exception& excp) {
      std::string logmsg = "";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
      excp.printStackTrace();
      FAIL("Function Execution Failed!");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopC1)
  {
    cleanProc();
    LOG("Clnt1Down complete: ");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, CloseServers)
  {
    // stop servers
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
    if (isLocalServer) {
      CacheHelper::closeServer(3);
      LOG("SERVER3 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATOR1, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

void runFunctionExecution() {
  CALL_TASK(StartLocator1);
  CALL_TASK(StartS13);
  CALL_TASK(StartC1);
  CALL_TASK(Client1OpTest2);
  CALL_TASK(StopC1);
  CALL_TASK(CloseServers);
  CALL_TASK(CloseLocator1);
}

DUNIT_MAIN
  { runFunctionExecution(); }
END_MAIN
