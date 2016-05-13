/*
 * The Execute Function QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Create the example Region Programmatically.
 * 3. Populate some objects on the Region.
 * 4. Create Execute Objects
 * 5. Execute Functions
 * 6. Close the Cache.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

char* getFuncIName = (char*)"MultiGetFunctionI";
char* putFuncIName = (char*)"MultiPutFunctionI";
char* getFuncName = (char*)"MultiGetFunction";
char* putFuncName = (char*)"MultiPutFunction";


// The Execute Function QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    // Create CacheFactory using the settings from the gfcpp.properties file by default.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    LOGINFO("Created CacheFactory");
    
    // Create a GemFire Cache.
    CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)->addServer("localhost", 50505)->addServer("localhost", 40404)->create();
    
    LOGINFO("Created the GemFire Cache");
    
    // Create the example Region Programmatically
    RegionPtr regPtr0 = cachePtr->createRegionFactory(CACHING_PROXY)->create("partition_region");
    
    LOGINFO("Created the Region");
    
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
    //test data independant function with result on one server
    LOGINFO("test data independant function with result on one server");
    CacheablePtr args = routingObj;
    ExecutionPtr exc = FunctionService::onServer((RegionServicePtr)cachePtr);
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
      sprintf(buf, "get: result count = %d", resultList->size());
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
    executeFunctionResult = 
         exc->withFilter(routingObj)->withArgs(args)->execute(getFuncName, getResult)->getResult();
    if(executeFunctionResult==NULLPTR)
    {
      LOGINFO( "execute on region: executeFunctionResult is NULL");
    } else 
    {
      resultList->clear();
      LOGINFO( "Execute on Region: result count = %d", executeFunctionResult->size());
      for(int32_t i=0; i < executeFunctionResult->size(); i++)
      {
        CacheableArrayListPtr arrayList = dynCast<CacheableArrayListPtr>(executeFunctionResult->operator[](i));
        for (int32_t pos=0; pos < arrayList->size(); pos++)
        {
          resultList->push_back(arrayList->operator[](pos));
        }
      }
      sprintf(buf, "Execute on Region: result count = %d", resultList->size());
      LOGINFO(buf);
      
      for(int32_t i=0; i < resultList->size(); i++)
      {
         sprintf(buf, "Execute on Region: result[%d]=%s", i, dynCast<CacheableStringPtr>(resultList->operator[](i))->asChar());
         LOGINFO(buf);
      }
    }

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
