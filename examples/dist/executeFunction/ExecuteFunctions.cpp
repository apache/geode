/*
 * The Execute Function Example.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Include the Execute function headers.
#include <gfcpp/FunctionService.hpp>
#include <gfcpp/Execution.hpp>
#include <gfcpp/ResultCollector.hpp>
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

// Use the "gemfire" namespace.
using namespace gemfire;

char* getFuncIName = (char*)"MultiGetFunctionI";
char* putFuncIName = (char*)"MultiPutFunctionI";
char* getFuncName = (char*)"MultiGetFunction";
char* putFuncName = (char*)"MultiPutFunction";
char* roFuncName = (char*)"RegionOperationsFunction";

//customer Result collector
class MyResultCollector : public ResultCollector
{
  public:
   MyResultCollector():
     m_resultList(CacheableVector::create()),
     m_isResultReady(false),
     m_endResultCount(0),
     m_addResultCount(0),
     m_getResultCount(0)
   {
   }
   ~MyResultCollector()
    {
    }
   CacheableVectorPtr getResult(uint32_t timeout )
   {
     m_getResultCount++;
     if(m_isResultReady == true)
       return m_resultList;
     else
     {
       for(uint32_t i=0; i < timeout; i++)
     {
#ifndef _WIN32
  sleep( 1 );
#else
  Sleep( 1 );
#endif
     if(m_isResultReady == true)
       return m_resultList;
     }
     throw FunctionExecutionException(
               "Result is not ready, endResults callback is called before invoking getResult() method");
     }
   }

   void addResult(CacheablePtr& result)
   {
     m_addResultCount++;
     if(result == NULLPTR)
      return;
     CacheableArrayListPtr results = dynCast<CacheableArrayListPtr>(result);
     for(int32_t i=0; i < results->size(); i++)
     {
       m_resultList->push_back(results->operator[](i));
     }
   }
   void endResults()
   {
     m_isResultReady = true;
     m_endResultCount++;
   }
   uint32_t getEndResultCount()
   {
     return m_endResultCount;
   }
   uint32_t getAddResultCount()
   {
     return m_addResultCount;
   }
   uint32_t getGetResultCount()
   {
     return m_getResultCount;
   }

  private:
    CacheableVectorPtr m_resultList;
    volatile bool m_isResultReady;
    uint32_t m_endResultCount;
    uint32_t m_addResultCount;
    uint32_t m_getResultCount;
};

// The Execute Function example.
int main(int argc, char ** argv)
{
  try
  {    
    // Create CacheFactory using the settings from the gfcpp.properties file by default. 
 	CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();   

    CachePtr cachePtr = cacheFactory
      ->setSubscriptionEnabled(true)
      ->addServer("localhost", 50505)
      ->addServer("localhost", 40404)
      ->create();

    LOGINFO("Created the GemFire Cache.");  

    RegionPtr regPtr0 = cachePtr
      ->createRegionFactory(CACHING_PROXY)
      ->create("partition_region");

    LOGINFO("Created the Partition Region.");  
    
    regPtr0->registerAllKeys();
    char buf[128];
    CacheableVectorPtr resultList = CacheableVector::create();
    for(int i=0; i < 34; i++)
    {
      sprintf(buf, "VALUE--%d", i);
      CacheablePtr value(CacheableString::create(buf));

      sprintf(buf, "KEY--%d", i);
      CacheableKeyPtr key = CacheableKey::create(buf);
      regPtr0->put(key, value);
    }

    bool getResult = true;
    CacheableVectorPtr routingObj = CacheableVector::create();
    for(int i=0; i < 34; i++)
    {
      if(i%2==0) continue;
      sprintf(buf, "KEY--%d", i);
      CacheableKeyPtr key = CacheableKey::create(buf);
      routingObj->push_back(key);
    }
    LOGINFO("test data independant function with result on one server");
    //test data independant function with result on one server
    ExecutionPtr exc = FunctionService::onServer((RegionServicePtr)cachePtr);
    CacheablePtr args = routingObj;
    CacheableVectorPtr executeFunctionResult = 
         exc->withArgs(args)->execute(getFuncIName, getResult)->getResult();
    if(executeFunctionResult==NULLPTR)
    {
      LOGINFO("get executeFunctionResult is NULL");
    } else
    {
      for (int32_t item=0; item < executeFunctionResult->size(); item++)
      {
        CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(executeFunctionResult->operator[](item));
        for (int32_t pos=0; pos < arrayList->size(); pos++)
        {
          resultList->push_back(arrayList->operator[](pos));
        }
      }
      sprintf(buf, "get: result count = %d", resultList->size());
      LOGINFO(buf);
      for(int32_t i=0; i < executeFunctionResult->size(); i++)
      {
         sprintf(buf, "get result[%d]=%s", i, dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
         LOGINFO(buf);
      }
    }

    LOGINFO("test data independant function without result on one server");
    getResult = false;
    exc->withArgs(args)->execute(putFuncIName, getResult, 15, false);

    LOGINFO("test data independant function with result on all servers");
    getResult = true;
    exc = FunctionService::onServers((RegionServicePtr)cachePtr);
    executeFunctionResult = 
         exc->withArgs(args)->execute(getFuncIName, getResult)->getResult();
    if(executeFunctionResult==NULLPTR)
    {
      LOGINFO("get executeFunctionResult is NULL");
    } else
    {
      resultList->clear();
      for (int32_t item=0; item < executeFunctionResult->size(); item++)
      {
        CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(executeFunctionResult->operator[](item));
        for (int32_t pos=0; pos < arrayList->size(); pos++)
        {
          resultList->push_back(arrayList->operator[](pos));
        }
      }
      sprintf(buf, "get result count = %d", resultList->size());
      LOGINFO(buf);
      for(int32_t i=0; i < executeFunctionResult->size(); i++)
      {
         sprintf(buf, "get result[%d]=%s", i, dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
         LOGINFO(buf);
      }
    }

    getResult = false;
    LOGINFO("test data independant function without result on all servers");
    exc->withArgs(args)->execute(putFuncIName, getResult, 15, false);

    LOGINFO("test data dependant function with result");
    getResult = true;
    args = CacheableBoolean::create( 1 );
    exc = FunctionService::onRegion(regPtr0);
    args = CacheableKey::create("echoString");
    executeFunctionResult = 
         exc->withFilter(routingObj)->withArgs(args)->execute(roFuncName, getResult, 15, true, true)->getResult();
    if(executeFunctionResult==NULLPTR)
    {
      LOGINFO("echo String : executeFunctionResult is NULL");
    } else 
    {
      LOGINFO("echo String : result count = %d", executeFunctionResult->size());
      const char* str = dynCast<CacheableStringPtr>(executeFunctionResult->operator[](0))->asChar();
      if(strcmp("echoString", str) != 0 ){
        LOGINFO("echoString is not echoed back");
      }
    }
    args = CacheableKey::create("echoBoolean");
    executeFunctionResult = 
         exc->withFilter(routingObj)->withArgs(args)->execute(roFuncName, getResult, 15, true, true)->getResult();
    if(executeFunctionResult==NULLPTR)
    {
      LOGINFO("echo Boolean: executeFunctionResult is NULL");
    } else 
    {
      LOGINFO("echo Boolean: result count = %d", executeFunctionResult->size());
      bool b = dynCast<CacheableBooleanPtr>(executeFunctionResult->operator[](0))->value();
      LOGINFO(b==true ? "true" : "false");
    }
    executeFunctionResult = 
         exc->withFilter(routingObj)->withArgs(args)->execute(getFuncName, getResult)->getResult();
    if(executeFunctionResult==NULLPTR)
    {
      LOGINFO( "execute on region: executeFunctionResult is NULL");
    } else 
    {
      resultList->clear();
      for (int32_t item=0; item < executeFunctionResult->size(); item++)
      {
        CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(executeFunctionResult->operator[](item));
        for (int32_t pos=0; pos < arrayList->size(); pos++)
        {
          resultList->push_back(arrayList->operator[](pos));
        }
      }
      LOGINFO( "Execute on Region: result count = %d", executeFunctionResult->size());
      for(int32_t i=0; i < executeFunctionResult->size(); i++)
      {
         sprintf(buf, "Execute on Region: result[%d]=%s", i, dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
         LOGINFO(buf);
      }
    }
    //     test get function with customer collector
    LOGINFO("test get function without customer collector");
    MyResultCollector *myRC = new MyResultCollector();
    executeFunctionResult = 
         exc->withFilter(routingObj)->withArgs(args)->withCollector(ResultCollectorPtr(myRC) )->execute(getFuncName, getResult)->getResult();
    LOGINFO("add result count = %d", myRC->getAddResultCount());
    LOGINFO("end result count = %d", myRC->getEndResultCount());
    LOGINFO("get result count = %d", myRC->getGetResultCount());

    LOGINFO("test data dependant function without result");
    getResult = false;
    exc->withFilter(routingObj)->withArgs(args)->execute(putFuncName, getResult, 15, false);
    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("Function Execution GemFire Exception: %s", gemfireExcp.getMessage());
  }
}

