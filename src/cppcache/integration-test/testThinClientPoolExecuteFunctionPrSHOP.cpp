/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientPoolExecuteFunctionPrSHOP"

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
bool isPoolWithEndpoint = false;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* poolRegNames[] = {"partition_region", "PoolRegion2"};

const char* serverGroup = "ServerGroup1";

char* getFuncIName = (char*)"MultiGetFunctionI";
char* putFuncIName = (char*)"MultiPutFunctionI";
char* getFuncName = (char*)"MultiGetFunction";
char* putFuncName = (char*)"MultiPutFunction";
char* rjFuncName = (char*)"RegionOperationsFunction";
char* exFuncName = (char*)"ExceptionHandlingFunction";
char* exFuncNameSendException = (char*)"executeFunction_SendException";
char* exFuncNamePdxType = (char*)"PdxFunctionTest";
char* FEOnRegionPrSHOP = (char*)"FEOnRegionPrSHOP";
char* FEOnRegionPrSHOP_OptimizeForWrite =
    (char*)"FEOnRegionPrSHOP_OptimizeForWrite";
char* FETimeOut = (char*)"FunctionExecutionTimeOut";

#define verifyGetResults()                                                    \
  bool found = false;                                                         \
  for (int j = 0; j < 34; j++) {                                              \
    if (j % 2 == 0) continue;                                                 \
    sprintf(buf, "VALUE--%d", j);                                             \
    if (strcmp(buf, dynCast<CacheableStringPtr>(resultList->operator[](i))    \
                        ->asChar()) == 0) {                                   \
      LOGINFO(                                                                \
          "buf = %s "                                                         \
          "dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar() " \
          "= %s ",                                                            \
          buf,                                                                \
          dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());  \
      found = true;                                                           \
      break;                                                                  \
    }                                                                         \
  }                                                                           \
  ASSERT(found, "this returned value is invalid");

#define verifyGetKeyResults()                                                 \
  bool found = false;                                                         \
  for (int j = 0; j < 34; j++) {                                              \
    if (j % 2 == 0) continue;                                                 \
    sprintf(buf, "KEY--%d", j);                                               \
    if (strcmp(buf, dynCast<CacheableStringPtr>(resultList->operator[](i))    \
                        ->asChar()) == 0) {                                   \
      LOGINFO(                                                                \
          "buf = %s "                                                         \
          "dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar() " \
          "= %s ",                                                            \
          buf,                                                                \
          dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());  \
      found = true;                                                           \
      break;                                                                  \
    }                                                                         \
  }                                                                           \
  ASSERT(found, "this returned KEY is invalid");

#define verifyPutResults()                   \
  bool found = false;                        \
  for (int j = 0; j < 34; j++) {             \
    if (j % 2 == 0) continue;                \
    sprintf(buf, "KEY--%d", j);              \
    if (strcmp(buf, value->asChar()) == 0) { \
      found = true;                          \
      break;                                 \
    }                                        \
  }                                          \
  ASSERT(found, "this returned value is invalid");
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

DUNIT_TASK_DEFINITION(LOCATOR1, StartLocator1)
  {
    // starting locator
    if (isLocator) {
      CacheHelper::initLocator(1);
      LOG("Locator1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, StartS12)
  {
    const char* lhp = NULL;
    if (!isPoolWithEndpoint) lhp = locHostPort;
    if (isLocalServer) {
      CacheHelper::initServer(1, "func_cacheserver1_pool.xml", lhp);
    }
    if (isLocalServer) {
      CacheHelper::initServer(2, "func_cacheserver2_pool.xml", lhp);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StartC1)
  {
    // initClient(true);
    initClientWithPool(true, NULL, locHostPort, serverGroup, NULLPTR, 0, true,
                       -1, -1, 60000, /*singlehop*/ true,
                       /*threadLocal*/ true);
    // createPool(poolName, locHostPort,serverGroup, NULL, 0, true );
    // createRegionAndAttachPool(poolRegNames[0],USE_ACK, poolName);

    RegionPtr regPtr0 =
        createRegionAndAttachPool(poolRegNames[0], USE_ACK, NULL);
    ;  // getHelper()->createRegion( poolRegNames[0], USE_ACK);
    regPtr0->registerAllKeys();

    LOG("Clnt1Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1OpTest)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(poolRegNames[0]);
    char buf[128];

    for (int i = 0; i < 34; i++) {
      sprintf(buf, "VALUE--%d", i);
      CacheablePtr value(CacheableString::create(buf));

      sprintf(buf, "KEY--%d", i);
      CacheableKeyPtr key = CacheableKey::create(buf);
      regPtr0->put(key, value);
    }
    SLEEP(10000);  // let the put finish
    try {
      CacheablePtr args = CacheableBoolean::create(1);
      bool getResult = true;
      CacheableVectorPtr routingObj = CacheableVector::create();
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        sprintf(buf, "KEY--%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        routingObj->push_back(key);
      }

      // test data dependant function
      //     test get function with result
      ExecutionPtr exc = FunctionService::onRegion(regPtr0);
      ASSERT(exc != NULLPTR, "onRegion Returned NULL");
      args = CacheableKey::create("echoString");
      CacheablePtr args1 = CacheableKey::create("echoBoolean");
      ExecutionPtr exe1 = exc->withArgs(args);
      ExecutionPtr exe2 = exe1->withArgs(args1);

      CacheableVectorPtr resultList = CacheableVector::create();
      CacheableVectorPtr executeFunctionResult =
          exe1->execute(rjFuncName, 15)->getResult();
      if (executeFunctionResult == NULLPTR) {
        ASSERT(false, "echo String : executeFunctionResult is NULL");
      } else {
        sprintf(buf, "echo String : result count = %d",
                executeFunctionResult->size());
        LOG(buf);
        try {
          CacheableStringPtr csp =
              dynCast<CacheableStringPtr>(executeFunctionResult->operator[](0));
          sprintf(buf, "echo String : cast successful, echoed string= %s",
                  csp->asChar());
          LOG(buf);
        } catch (ClassCastException& ex) {
          std::string logmsg = "";
          logmsg += ex.getName();
          logmsg += ": ";
          logmsg += ex.getMessage();
          LOG(logmsg.c_str());
          ex.printStackTrace();
          LOG("cast to string failed, now cast to boolean:");
          bool b ATTR_UNUSED =
              dynCast<CacheableBooleanPtr>(executeFunctionResult->operator[](0))
                  ->value();
          ASSERT(false, "echo String : wrong argument type");
        }
      }
      executeFunctionResult = exc->withFilter(routingObj)
                                  ->withArgs(args)
                                  ->execute(exFuncName, 15)
                                  ->getResult();

      executeFunctionResult = exc->withFilter(routingObj)
                                  ->withArgs(args)
                                  ->execute(rjFuncName, 15)
                                  ->getResult();
      if (executeFunctionResult == NULLPTR) {
        ASSERT(false, "echo String : executeFunctionResult is NULL");
      } else {
        sprintf(buf, "echo String : result count = %d",
                executeFunctionResult->size());
        LOG(buf);
        const char* str =
            dynCast<CacheableStringPtr>(executeFunctionResult->operator[](0))
                ->asChar();
        LOG(str);
        ASSERT(strcmp("echoString", str) == 0, "echoString is not eched back");
      }
      args = CacheableKey::create("echoBoolean");
      executeFunctionResult = exc->withFilter(routingObj)
                                  ->withArgs(args)
                                  ->execute(rjFuncName, 15)
                                  ->getResult();
      if (executeFunctionResult == NULLPTR) {
        ASSERT(false, "echo Boolean: executeFunctionResult is NULL");
      } else {
        sprintf(buf, "echo Boolean: result count = %d",
                executeFunctionResult->size());
        LOG(buf);
        bool b =
            dynCast<CacheableBooleanPtr>(executeFunctionResult->operator[](0))
                ->value();
        LOG(b == true ? "true" : "false");
        ASSERT(b == true, "true is not eched back");
      }

      executeFunctionResult = exc->withFilter(routingObj)
                                  ->withArgs(args)
                                  ->execute(getFuncName, getResult)
                                  ->getResult();
      /****
       **decomposed from above long expression:
      exc =  exc->withFilter(routingObj);
      ASSERT(exc!=NULLPTR, "withFilter Returned NULL");
      exc = exc->withArgs(args);
      ASSERT(exc!=NULLPTR, "withArgs Returned NULL");
      ResultCollectorPtr rc = exc->execute(getFuncName, getResult);
      ASSERT(rc!=NULLPTR, "execute Returned NULL");
      CacheableVectorPtr executeFunctionResult = rc->getResult();
      */
      if (executeFunctionResult == NULLPTR) {
        ASSERT(false, "region get: executeFunctionResult is NULL");
      } else {
        for (unsigned item = 0;
             item < static_cast<uint32_t>(executeFunctionResult->size());
             item++) {
          CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
              executeFunctionResult->operator[](item));
          for (unsigned pos = 0; pos < static_cast<uint32_t>(arrayList->size());
               pos++) {
            resultList->push_back(arrayList->operator[](pos));
          }
        }
        sprintf(buf, "Region get: result count = %d", resultList->size());
        LOG(buf);
        ASSERT(resultList->size() == 34,
               "region get: resultList count is not 34");
        for (int32_t i = 0; i < resultList->size(); i++) {
          CacheableStringPtr csPtr =
              dynCast<CacheableStringPtr>(resultList->operator[](i));
          //  printf(" in csPtr = %u \n",csPtr);
          if (csPtr != NULLPTR) {
            sprintf(buf, "Region get: result[%d]=%s", i,
                    dynCast<CacheableStringPtr>(resultList->operator[](i))
                        ->asChar());
            LOG(buf);
            verifyGetResults()
          } else {
            CacheablePtr tmp = resultList->operator[](i);
            if (tmp != NULLPTR) {
              printf(" in typeid = %d  \n", tmp->typeId());

            } else {
              printf(" in typeid is null \n");
            }
          }
        }
      }
      //     test get function with customer collector
      MyResultCollectorPtr myRC(new MyResultCollector());
      executeFunctionResult = exc->withFilter(routingObj)
                                  ->withArgs(args)
                                  ->withCollector(myRC)
                                  ->execute(getFuncName, getResult)
                                  ->getResult();
      sprintf(buf, "add result count = %d", myRC->getAddResultCount());
      LOG(buf);
      sprintf(buf, "end result count = %d", myRC->getEndResultCount());
      LOG(buf);
      sprintf(buf, "get result count = %d", myRC->getGetResultCount());
      LOG(buf);
      if (executeFunctionResult == NULLPTR) {
        ASSERT(false,
               "region get new collector: executeFunctionResult is NULL");
      } else {
        resultList->clear();
        for (unsigned item = 0;
             item < static_cast<uint32_t>(executeFunctionResult->size());
             item++) {
          resultList->push_back(executeFunctionResult->operator[](item));
        }
        sprintf(buf, "Region get new collector: result list count = %d",
                resultList->size());
        LOG(buf);
        sprintf(buf, "Region get new collector: result count = %d",
                executeFunctionResult->size());
        LOG(buf);
        ASSERT(
            executeFunctionResult->size() == resultList->size(),
            "region get new collector: executeFunctionResult count is not 34");
        for (int32_t i = 0; i < executeFunctionResult->size(); i++) {
          sprintf(
              buf, "Region get new collector: result[%d]=%s", i,
              dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
          LOG(buf);
          verifyGetResults()
        }
      }
      LOG("Done.................");
      //     test put function without result
      getResult = false;
      exc->withFilter(routingObj)->withArgs(args)->execute(putFuncName, 15);
      LOG("Done.................2");
      SLEEP(10000);  // let the put finish
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        sprintf(buf, "KEY--%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        CacheableStringPtr value =
            dynCast<CacheableStringPtr>(regPtr0->get(key));
        sprintf(buf, "Region put: result[%d]=%s", i, value->asChar());
        LOG(buf);
        verifyPutResults()
      }

      args = routingObj;
      getResult = true;
      executeFunctionResult = exc->withArgs(args)
                                  ->withFilter(routingObj)
                                  ->execute(getFuncName, 15)
                                  ->getResult();

      if (executeFunctionResult == NULLPTR) {
        ASSERT(false, "get executeFunctionResult is NULL");
      } else {
        sprintf(buf, "echo String : result count = %d",
                executeFunctionResult->size());
        LOGINFO(buf);
        resultList->clear();

        for (unsigned item = 0;
             item < static_cast<uint32_t>(executeFunctionResult->size());
             item++) {
          CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
              executeFunctionResult->operator[](item));
          for (unsigned pos = 0; pos < static_cast<uint32_t>(arrayList->size());
               pos++) {
            resultList->push_back(arrayList->operator[](pos));
          }
        }
        sprintf(buf, "get result count = %d", resultList->size());
        LOGINFO(buf);
        ASSERT(resultList->size() == 34,
               "get executeFunctionResult count is not 34");
        for (int32_t i = 0; i < resultList->size(); i++) {
          sprintf(buf, "result[%d] is null\n", i);
          ASSERT(resultList->operator[](i) != NULLPTR, buf);
          sprintf(
              buf, "get result[%d]=%s", i,
              dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
          LOGINFO(buf);
          verifyGetKeyResults()
        }
      }

      //-----------------------Test with sendException
      // onRegion-------------------------------//
      for (int i = 1; i <= 200; i++) {
        CacheablePtr value(CacheableInt32::create(i));

        sprintf(buf, "execKey-%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        regPtr0->put(key, value);
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

      args = CacheableBoolean::create(1);
      getResult = true;

      ExecutionPtr funcExec = FunctionService::onRegion(regPtr0);
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
      LOGINFO("result->size() = %d & arrList->size()  = %d ", result->size(),
              arrList->size() + 1);
      ASSERT(
          result->size() == arrList->size() + 1,
          "region get: resultList count is not as arrayList count + exception");

      for (int i = 0; i < result->size(); i++) {
        try {
          CacheableInt32Ptr intValue =
              dynCast<CacheableInt32Ptr>(result->operator[](i));
          ASSERT(intValue != NULLPTR, "int value is NULL");
          LOGINFO("intValue is %d ", intValue->value());
        } catch (ClassCastException& ex) {
          LOG("exFuncNameSendException casting to int for arrayList arguement "
              "exception.");
          std::string logmsg = "";
          logmsg += ex.getName();
          logmsg += ": ";
          logmsg += ex.getMessage();
          LOG(logmsg.c_str());
          ex.printStackTrace();
          LOG("exFuncNameSendException now casting to "
              "UserFunctionExecutionExceptionPtr for arrayList arguement "
              "exception.");
          UserFunctionExecutionExceptionPtr uFEPtr =
              dynCast<UserFunctionExecutionExceptionPtr>(result->operator[](i));
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

      LOG("exFuncNameSendException for string arguement.");

      args = CacheableString::create("Multiple");
      collector = funcExec->withArgs(args)->withFilter(filter)->execute(
          exFuncNameSendException, 15);
      ASSERT(collector != NULLPTR, "onRegion collector for string NULL");
      result = collector->getResult();
      LOGINFO("result->size() for Multiple String = %d ", result->size());
      ASSERT(result->size() == 1,
             "region get: resultList count is not as string exception");

      try {
        for (int i = 0; i < result->size(); i++) {
          LOG("exFuncNameSendException now casting to "
              "UserFunctionExecutionExceptionPtr for arrayList arguement "
              "exception.");
          UserFunctionExecutionExceptionPtr uFEPtr =
              dynCast<UserFunctionExecutionExceptionPtr>(result->operator[](i));
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
            "exFuncNameSendException casting to string for string arguement "
            "exception.");
      } catch (...) {
        FAIL(
            "exFuncNameSendException casting to string for string arguement "
            "Unknown exception.");
      }

      LOG("exFuncNameSendException for string arguement done.");

      LOGINFO(
          "test exFuncNameSendException function with customer collector with "
          "bool as arguement using onRegion.");

      MyResultCollectorPtr myRC1(new MyResultCollector());
      result = funcExec->withArgs(args)
                   ->withFilter(filter)
                   ->withCollector(myRC1)
                   ->execute(exFuncNameSendException, getResult)
                   ->getResult();
      LOGINFO("add result count = %d", myRC1->getAddResultCount());
      LOGINFO("end result count = %d", myRC1->getEndResultCount());
      LOGINFO("get result count = %d", myRC1->getGetResultCount());
      ASSERT(1 == myRC1->getAddResultCount(), "add result count is not 1");
      ASSERT(1 == myRC1->getEndResultCount(), "end result count is not 1");
      ASSERT(1 == myRC1->getGetResultCount(), "get result count is not 1");
      if (result == NULLPTR) {
        ASSERT(false, "region get new collector: result is NULL");
      } else {
        LOGINFO("Region get new collector: result count = %d", result->size());
        ASSERT(1 == result->size(),
               "region get new collector: result count is not 1");
      }

      LOGINFO(
          "test exFuncNameSendException function with customer collector with "
          "bool as arguement done using onRegion.");

      //-----------------------Test with sendException onRegion
      // done-------------------------------//

      // restore the data set
      for (int i = 0; i < 34; i++) {
        sprintf(buf, "VALUE--%d", i);
        CacheablePtr value(CacheableString::create(buf));

        sprintf(buf, "KEY--%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        regPtr0->put(key, value);
      }

      // test data independant function
      //     test get function with result
      getResult = true;
      //    PoolPtr pptr = PoolManager::find(poolName);
      args = routingObj;
      // ExecutionPtr exc=NULLPTR;
      // CacheableVectorPtr executeFunctionResult = NULLPTR;
      // test data independant function on one server
      LOG("test data independant get function on one server");
      exc = FunctionService::onServer(getHelper()->cachePtr);
      executeFunctionResult =
          exc->withArgs(args)->execute(getFuncIName, getResult)->getResult();
      if (executeFunctionResult == NULLPTR) {
        ASSERT(false, "get executeFunctionResult is NULL");
      } else {
        resultList->clear();
        for (unsigned item = 0;
             item < static_cast<uint32_t>(executeFunctionResult->size());
             item++) {
          CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
              executeFunctionResult->operator[](item));
          for (unsigned pos = 0; pos < static_cast<uint32_t>(arrayList->size());
               pos++) {
            resultList->push_back(arrayList->operator[](pos));
          }
        }
        sprintf(buf, "get result count = %d", resultList->size());
        LOG(buf);
        ASSERT(resultList->size() == 17,
               "get executeFunctionResult count is not 17");
        for (int32_t i = 0; i < resultList->size(); i++) {
          sprintf(buf, "result[%d] is null\n", i);
          ASSERT(resultList->operator[](i) != NULLPTR, buf);
          sprintf(
              buf, "get result[%d]=%s", i,
              dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
          LOG(buf);
          verifyGetResults()
        }
      }
      LOG("test data independant put function on one server");
      //     test put function without result
      getResult = false;
      exc->withArgs(args)->execute(putFuncIName, 15);
      SLEEP(500);  // let the put finish
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        sprintf(buf, "KEY--%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        CacheableStringPtr value =
            dynCast<CacheableStringPtr>(regPtr0->get(key));
        sprintf(buf, "put: result[%d]=%s", i, value->asChar());
        LOG(buf);
        verifyPutResults()
      }
      LOG("test data independant get function on all servers");
      // test data independant function on all servers
      // test get

      //-----------------------Test with PdxObject
      // onServers-------------------------------//

      getResult = true;
      try {
        Serializable::registerPdxType(
            PdxTests::PdxTypes8::createDeserializable);
      } catch (const IllegalStateException&) {
        // ignore exception
      }

      PdxTypes8Ptr pdxobj(new PdxTests::PdxTypes8());
      for (int i = 0; i < 34; i++) {
        sprintf(buf, "KEY--pdx%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        regPtr0->put(key, pdxobj);
      }
      LOGINFO("put on pdxObject done");

      CacheableVectorPtr pdxRoutingObj = CacheableVector::create();
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        sprintf(buf, "KEY--pdx%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        pdxRoutingObj->push_back(key);
      }

      ExecutionPtr pdxExc = FunctionService::onServers(getHelper()->cachePtr);
      CacheableVectorPtr executeFunctionResultPdx =
          pdxExc->withArgs(pdxRoutingObj)
              ->execute(exFuncNamePdxType, getResult)
              ->getResult();
      LOGINFO("FE on pdxObject done");
      if (executeFunctionResultPdx == NULLPTR) {
        ASSERT(false, "get executeFunctionResultPdx is NULL");
      } else {
        LOGINFO("executeFunctionResultPdx size for PdxObject = %d ",
                executeFunctionResultPdx->size());
        ASSERT(2 == executeFunctionResultPdx->size(),
               "executeFunctionResultPdx->size() is not 2");
        CacheableVectorPtr resultListPdx = CacheableVector::create();
        for (unsigned item = 0;
             item < static_cast<uint32_t>(executeFunctionResultPdx->size());
             item++) {
          CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
              executeFunctionResultPdx->operator[](item));
          for (unsigned pos = 0; pos < static_cast<uint32_t>(arrayList->size());
               pos++) {
            resultListPdx->push_back(arrayList->operator[](pos));
          }
        }
        LOGINFO("resultlistPdx size: %d", resultListPdx->size());
        for (int32_t i = 0; i < resultListPdx->size(); i++) {
          sprintf(buf, "result[%d] is null\n", i);
          ASSERT(resultListPdx->operator[](i) != NULLPTR, buf);
          LOG("resultPdx item is not null");
          LOGINFO("get result[%d]=%s", i,
                  dynCast<PdxTypes8Ptr>(resultListPdx->operator[](i))
                      ->toString()
                      ->asChar());
          PdxTypes8Ptr pdxObj2 =
              dynCast<PdxTypes8Ptr>(resultListPdx->operator[](i));
          ASSERT(pdxobj->equals(pdxObj2) == true,
                 "Pdx Object not equals original object.");
        }
      }
      LOG("test pdx data function on all servers done");

      PdxInstanceFactoryPtr pifPtr =
          cacheHelper->getCache()->createPdxInstanceFactory(
              "PdxTests.PdxTypes8");
      LOG("PdxInstanceFactoryPtr created....");

      pifPtr->writeInt("i1", 1);
      pifPtr->writeInt("i2", 2);
      pifPtr->writeString("s1", "m_s1");
      pifPtr->writeString("s2", "m_s2");
      pifPtr->writeInt("i3", 3);
      pifPtr->writeInt("i4", 4);

      PdxInstancePtr ret = pifPtr->create();
      LOG("PdxInstancePtr created");

      for (int i = 0; i < 34; i++) {
        sprintf(buf, "KEY--pdx%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        regPtr0->put(key, pdxobj);
      }
      LOGINFO("put on pdxObject done");

      CacheableVectorPtr pdxInstanceRoutingObj = CacheableVector::create();
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        sprintf(buf, "KEY--pdx%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        pdxInstanceRoutingObj->push_back(key);
      }

      ExecutionPtr pdxInstanceExc =
          FunctionService::onServers(getHelper()->cachePtr);
      CacheableVectorPtr executeFunctionResultPdxInstance =
          pdxInstanceExc->withArgs(pdxInstanceRoutingObj)
              ->execute(exFuncNamePdxType, getResult)
              ->getResult();
      LOGINFO("FE on pdxObject done");
      if (executeFunctionResultPdxInstance == NULLPTR) {
        ASSERT(false, "get executeFunctionResultPdxInstance is NULL");
      } else {
        LOGINFO("executeFunctionResultPdxInstance size for PdxObject = %d ",
                executeFunctionResultPdxInstance->size());
        ASSERT(2 == executeFunctionResultPdx->size(),
               "executeFunctionResultPdxInstance->size() is not 2");
        CacheableVectorPtr resultListPdxInstance = CacheableVector::create();
        for (unsigned item = 0;
             item <
             static_cast<uint32_t>(executeFunctionResultPdxInstance->size());
             item++) {
          CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
              executeFunctionResultPdxInstance->operator[](item));
          for (unsigned pos = 0; pos < static_cast<uint32_t>(arrayList->size());
               pos++) {
            resultListPdxInstance->push_back(arrayList->operator[](pos));
          }
        }
        LOGINFO("resultlistPdxInstance size: %d",
                resultListPdxInstance->size());
        for (int32_t i = 0; i < resultListPdxInstance->size(); i++) {
          sprintf(buf, "result[%d] is null\n", i);
          ASSERT(resultListPdxInstance->operator[](i) != NULLPTR, buf);
          LOG("resultPdx item is not null");
          LOGINFO("get result[%d]=%s", i,
                  dynCast<PdxTypes8Ptr>(resultListPdxInstance->operator[](i))
                      ->toString()
                      ->asChar());
          PdxTypes8Ptr pdxObj2 =
              dynCast<PdxTypes8Ptr>(resultListPdxInstance->operator[](i));

          ASSERT(pdxobj->equals(pdxObj2) == true,
                 "Pdx Object not equals original object.");
        }
      }
      LOG("test pdx data function on all servers done");

      //-----------------------Test with PdxObject onServers
      // done.-------------------------------//

      // repopulate with the original values
      for (int i = 0; i < 34; i++) {
        sprintf(buf, "VALUE--%d", i);
        CacheablePtr value(CacheableString::create(buf));

        sprintf(buf, "KEY--%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        regPtr0->put(key, value);
      }
      SLEEP(10000);  // let the put finish
      //---------------------------------------------------------------------
      // adding one more server, this should be return by locator and then
      // function should be executed on all the 3 servers

      const char* lhp = NULL;
      if (!isPoolWithEndpoint) lhp = locHostPort;

      if (isLocalServer) {
        CacheHelper::initServer(3, "func_cacheserver3_pool.xml", lhp);
      }
      SLEEP(60000);  // let this servers gets all the data

      //---------------------------------------------------------------------
      getResult = true;
      exc = FunctionService::onServers(getHelper()->cachePtr);
      executeFunctionResult =
          exc->withArgs(args)->execute(getFuncIName, getResult)->getResult();
      if (executeFunctionResult == NULLPTR) {
        ASSERT(false, "get executeFunctionResult is NULL");
      } else {
        resultList->clear();
        for (unsigned item = 0;
             item < static_cast<uint32_t>(executeFunctionResult->size());
             item++) {
          CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(
              executeFunctionResult->operator[](item));
          for (unsigned pos = 0; pos < static_cast<uint32_t>(arrayList->size());
               pos++) {
            resultList->push_back(arrayList->operator[](pos));
          }
        }
        sprintf(buf, "get result count = %d", resultList->size());
        LOG(buf);
        printf("resultlist size: %d", resultList->size());
        ASSERT(resultList->size() == 51,
               "get executeFunctionResult on all servers count is not 51");
        for (int32_t i = 0; i < resultList->size(); i++) {
          sprintf(buf, "result[%d] is null\n", i);
          ASSERT(resultList->operator[](i) != NULLPTR, buf);
          sprintf(
              buf, "get result[%d]=%s", i,
              dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
          LOG(buf);
          verifyGetResults()
        }
      }
      LOG("test data independant put function on all servers");
      // test data independant function on all servers
      // test put
      getResult = false;
      exc->withArgs(args)->execute(putFuncIName, 15);
      SLEEP(10000);  // let the put finish
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        sprintf(buf, "KEY--%d", i);
        CacheableKeyPtr key = CacheableKey::create(buf);
        CacheableStringPtr value =
            dynCast<CacheableStringPtr>(regPtr0->get(key));
        sprintf(buf, "put: result[%d]=%s", i, value->asChar());
        LOG(buf);
        verifyPutResults()
      }
      //-----------------------Test with sendException
      // onServers-------------------------------//
      LOG("OnServers with sendException");
      args = CacheableBoolean::create(1);
      getResult = true;
      funcExec = FunctionService::onServers(getHelper()->cachePtr);

      collector =
          funcExec->withArgs(args)->execute(exFuncNameSendException, 15);
      ASSERT(collector != NULLPTR, "onServers collector for bool NULL");

      result = collector->getResult();
      ASSERT(result->size() == 3,
             "Should have got 3 exception strings for sendException.");
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

      collector =
          funcExec->withArgs(arrList)->execute(exFuncNameSendException, 15);
      ASSERT(collector != NULLPTR, "onServers collector for arrList NULL");

      result = collector->getResult();
      ASSERT(result->size() == (arrList->size() + 1) * 3,
             "onServers get: resultList count is not as arrayList count + "
             "exception");

      LOG("Printing only string exception results for onServers with "
          "sendException");
      for (int i = 0; i < result->size(); i++) {
        try {
          CacheableInt32Ptr intValue =
              dynCast<CacheableInt32Ptr>(result->operator[](i));
          ASSERT(intValue != NULLPTR, "int value is NULL");
          LOGINFO("intValue is %d ", intValue->value());
        } catch (ClassCastException& ex) {
          std::string logmsg = "";
          logmsg += ex.getName();
          logmsg += ": ";
          logmsg += ex.getMessage();
          LOG(logmsg.c_str());
          ex.printStackTrace();
          UserFunctionExecutionExceptionPtr uFEPtr =
              dynCast<UserFunctionExecutionExceptionPtr>(result->operator[](i));
          ASSERT(uFEPtr != NULLPTR, "uFEPtr exception is NULL");
          LOGINFO("Done casting to uFEPtr");
          LOGINFO("Read expected uFEPtr exception %s ",
                  uFEPtr->getMessage()->asChar());
        } catch (...) {
          FAIL(
              "exFuncNameSendException casting to string for arrList arguement "
              "for onServers Unknown exception.");
        }
      }

      LOG("Printing all results for onServers with sendException");
      for (int i = 0; i < result->size(); i++) {
        try {
          CacheableInt32Ptr intValue =
              dynCast<CacheableInt32Ptr>(result->operator[](i));
          ASSERT(intValue != NULLPTR, "int value is NULL");
          LOGINFO("intValue is %d ", intValue->value());
        } catch (ClassCastException& ex) {
          LOG("exFuncNameSendException casting to int for arrayList arguement "
              "exception.");
          std::string logmsg = "";
          logmsg += ex.getName();
          logmsg += ": ";
          logmsg += ex.getMessage();
          LOG(logmsg.c_str());
          ex.printStackTrace();
          LOG("exFuncNameSendException now casting to string for arrayList "
              "arguement exception.");
          UserFunctionExecutionExceptionPtr uFEPtr =
              dynCast<UserFunctionExecutionExceptionPtr>(result->operator[](i));
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

      LOG("exFuncNameSendException for string arguement.");

      args = CacheableString::create("Multiple");
      collector =
          funcExec->withArgs(args)->execute(exFuncNameSendException, 15);
      ASSERT(collector != NULLPTR, "onServers collector for string NULL");

      result = collector->getResult();
      ASSERT(result->size() == 3,
             "region get: resultList count is not as string exception");

      try {
        for (int i = 0; i < result->size(); i++) {
          UserFunctionExecutionExceptionPtr uFEPtr =
              dynCast<UserFunctionExecutionExceptionPtr>(result->operator[](i));
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
            "exFuncNameSendException casting to string for string arguement "
            "exception.");
      } catch (...) {
        FAIL(
            "exFuncNameSendException casting to string for string arguement "
            "Unknown exception.");
      }

      LOG("exFuncNameSendException for string arguement done.");

      LOGINFO(
          "test exFuncNameSendException function with customer collector with "
          "bool as arguement using onServers.");

      MyResultCollectorPtr myRC2(new MyResultCollector());
      result = funcExec->withArgs(args)
                   ->withCollector(myRC2)
                   ->execute(exFuncNameSendException, getResult)
                   ->getResult();
      ASSERT(3 == myRC2->getAddResultCount(), "add result count is not 3");
      ASSERT(1 == myRC2->getEndResultCount(), "end result count is not 1");
      ASSERT(1 == myRC2->getGetResultCount(), "get result count is not 1");
      LOGINFO("add result count = %d", myRC2->getAddResultCount());
      LOGINFO("end result count = %d", myRC2->getEndResultCount());
      LOGINFO("get result count = %d", myRC2->getGetResultCount());
      if (result == NULLPTR) {
        ASSERT(false, "region get new collector: result is NULL");
      } else {
        LOGINFO("Region get new collector: result count = %d", result->size());
        ASSERT(3 == result->size(),
               "region get new collector: result count is not 1");
      }

      LOGINFO(
          "test exFuncNameSendException function with customer collector with "
          "bool as arguement done using onServers.");

      LOG("OnServers with sendException done");

      //-----------------------Test with sendException onServers
      // done.-------------------------------//

      //-----------------------Test with single filter key
      // onRegion-------------------------------//
      for (int i = 0; i < 230; i++) {
        sprintf(buf, "VALUE--%d", i);
        CacheablePtr value(CacheableString::create(buf));
        regPtr0->put(i, value);
      }
      LOGINFO("Put done.");

      getResult = true;
      for (int i = 0; i < 230; i++) {
        CacheableVectorPtr fil = CacheableVector::create();
        fil->push_back(CacheableInt32::create(i));
        ExecutionPtr exe = FunctionService::onRegion(regPtr0);
        CacheableVectorPtr executeFunctionResult2 =
            exe->withFilter(fil)->execute(FEOnRegionPrSHOP, 15)->getResult();
        if (executeFunctionResult2 == NULLPTR) {
          ASSERT(false, "executeFunctionResult2 is NULLPTR");
        } else {
          sprintf(buf, "executeFunctionResult2 count = %d",
                  executeFunctionResult2->size());
          LOGINFO(buf);
          ASSERT(1 == executeFunctionResult2->size(),
                 "executeFunctionResult2->size() is not 1");
          for (int i = 0; i < executeFunctionResult2->size(); i++) {
            bool b = dynCast<CacheableBooleanPtr>(
                         executeFunctionResult2->operator[](i))
                         ->value();
            LOGINFO(b == true ? "true" : "false");
            ASSERT(b == true, "true is not eched back");
          }
        }
        executeFunctionResult2->clear();
        LOGINFO("FEOnRegionPrSHOP without Filter done");

        CacheableVectorPtr functionResult =
            exe->withFilter(fil)
                ->execute(FEOnRegionPrSHOP_OptimizeForWrite, 15)
                ->getResult();
        if (functionResult == NULLPTR) {
          ASSERT(false, "functionResult is NULLPTR");
        } else {
          sprintf(buf, "result count = %d", functionResult->size());
          LOGINFO(buf);
          ASSERT(1 == functionResult->size(),
                 "functionResult->size() is not 1");
          for (int i = 0; i < functionResult->size(); i++) {
            bool b = dynCast<CacheableBooleanPtr>(functionResult->operator[](i))
                         ->value();
            LOGINFO(b == true ? "true" : "false");
            ASSERT(b == true, "true is not eched back");
          }
        }
        LOGINFO("FEOnRegionPrSHOP_OptimizeForWrite without Filter done");
      }
      //-----------------------Test with single filter key onRegion
      // done-------------------------------//

      //////////////////////OnRegion TimeOut ///////////////////////////
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
        // ASSERT(2  == fe->size(), "FunctionResult->size() is not 2");
        for (int i = 0; i < fe->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(fe->operator[](i))->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      LOGINFO("FETimeOut done onRegion");
      //////////////////////OnRegion TimeOut Done///////////////////////////

      LOGINFO("FETimeOut begin onServer");
      ExecutionPtr serverExc = FunctionService::onServer(getHelper()->cachePtr);
      CacheableVectorPtr vec = serverExc->withArgs(CacheableInt32::create(5000))
                                   ->execute(FETimeOut, 5000)
                                   ->getResult();
      if (vec == NULLPTR) {
        ASSERT(false, "functionResult is NULL");
      } else {
        sprintf(buf, "result count = %d", vec->size());
        LOG(buf);
        ASSERT(1 == vec->size(), "FunctionResult->size() is not 1");
        for (int i = 0; i < vec->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(vec->operator[](i))->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      LOGINFO("FETimeOut done onServer");

      LOGINFO("FETimeOut begin onServers");
      ExecutionPtr serversExc =
          FunctionService::onServers(getHelper()->cachePtr);
      CacheableVectorPtr vecs =
          serversExc->withArgs(CacheableInt32::create(5000))
              ->execute(FETimeOut, 5000)
              ->getResult();
      if (vecs == NULLPTR) {
        ASSERT(false, "functionResult is NULL");
      } else {
        sprintf(buf, "result count = %d", vecs->size());
        LOG(buf);
        ASSERT(3 == vecs->size(), "FunctionResult->size() is not 3");
        for (int i = 0; i < vecs->size(); i++) {
          bool b = dynCast<CacheableBooleanPtr>(vecs->operator[](i))->value();
          LOG(b == true ? "true" : "false");
          ASSERT(b == true, "true is not eched back");
        }
      }
      LOGINFO("FETimeOut done onServers");

    } catch (const Exception& excp) {
      std::string logmsg = "";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
      excp.printStackTrace();
      FAIL("Function Execution Failed!");
    }
    isPoolWithEndpoint = true;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopC1)
  {
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
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
    isPoolWithEndpoint = true;
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

void runFunctionExecution(bool isEndpoint) {
  // with locator
  CALL_TASK(StartLocator1);
  CALL_TASK(StartS12);
  CALL_TASK(StartC1);
  CALL_TASK(Client1OpTest);
  CALL_TASK(StopC1);
  CALL_TASK(CloseServers);
  CALL_TASK(CloseLocator1);

  // with endpoints
  CALL_TASK(StartS12);
  CALL_TASK(StartC1);
  CALL_TASK(Client1OpTest);
  CALL_TASK(StopC1);
  CALL_TASK(CloseServers);
}

DUNIT_MAIN
  { runFunctionExecution(false); }
END_MAIN
