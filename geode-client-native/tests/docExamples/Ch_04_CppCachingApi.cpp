/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_04_CppCachingApi.hpp"
#include "statistics/StatisticsFactory.hpp"

using namespace gemfire;
using namespace gemfire_statistics;
using namespace docExample;

//constructor
CppCachingApi::CppCachingApi()
{
}

//destructor
CppCachingApi::~CppCachingApi()
{
}

//start cacheserver
void CppCachingApi::startServer()
{
  CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
}

//stop cacheserver
void CppCachingApi::stopServer()
{
  CacheHelper::closeServer( 1 );
}

/**
* @brief Example 4.1 Configuring client. It allows to set default Pool Properties
*/
void CppCachingApi::example_4_1()
{
  cacheFactoryPtr = CacheFactory::createCacheFactory();
}

/**
* @brief Example 4.2 Creating a Cache.
* In the next example, the application creates the cache by calling the CacheFactory::create
*/
void CppCachingApi::example_4_2()
{
    cachePtr = cacheFactoryPtr->setSubscriptionEnabled(true)
        ->addServer("localhost", 24680)
        ->create();
}

/**
* @brief Example 4.3 Creating a Region With Caching and LRU.
* The following example shows how to create a region with caching and LRU enabled. The application
* overrides the default region attributes where necessary
*/
void CppCachingApi::example_4_3()
{
  /** create the region */
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr = regionFactory->setLruEntriesLimit( 20000 )
      ->setInitialCapacity( 20000 )
      ->create("exampleRegion");
}

/**
* @brief Example 4.4 Creating a Region With Disk Overflow Based on Entry Capacity.
* The following example shows how to programmatically set up the attributes during region.
*/
void CppCachingApi::example_4_4()
{
  /** set up some region attributes */
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  PropertiesPtr bdbProperties = Properties::create();
  bdbProperties->insert("PersistenceDirectory", "BDB");
  bdbProperties->insert("EnvironmentDirectory", "BDBEnv");
  //Use either of the two for setting the size
  bdbProperties->insert("CacheSizeGb", "1");
  //or use this
  //bdbProperties->insert("CacheSizeMb","512");
  bdbProperties->insert("PageSize", "65536");
  bdbProperties->insert("MaxFileSize","512000");
  RegionPtr regionPtr = regionFactory->setCachingEnabled(true)
           ->setLruEntriesLimit( 20000 )
           ->setInitialCapacity( 20000 )
           ->setDiskPolicy( DiskPolicyType::OVERFLOWS )
           ->setPersistenceManager("BDBImpl","createBDBInstance",bdbProperties)
           ->create("exampleRegion");
}

/**
* @brief Example 4.5 Using the API to Put Entries Into the Cache.
* In the next example, the program uses the API to put 100
* entries into the cache by iteratively creating keys and values, both of which are integers.
*/
void CppCachingApi::example_4_5()
{
  for ( int32_t i=0; i < 100; i++ ) {
    regionPtr->put( i, CacheableInt32::create(i) );
  }
}

/**
* @brief Example 4.6 Using the get API to Retrieve Values From the Cache.
* In the following example, the program uses the API to do a get for each entry that was put into
* the cache.
*/
void CppCachingApi::example_4_6()
{
  for ( int32_t i=0; i< 100; i++) {
    CacheableInt32Ptr res = dynCast<CacheableInt32Ptr>(regionPtr->get(i));
  }
}

/**
* @brief Example 4.12 Creating New Statistics Programmatically.
* The following example provides a programmatic code sample for creating and registering new
* statistics.
*/
void CppCachingApi::example_4_12()
{
  //Get StatisticsFactory
  StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
  //Define each StatisticDescriptor and put each in an array
  StatisticDescriptor** statDescriptorArr = new StatisticDescriptor*[6];
  statDescriptorArr[0] = factory->createIntCounter("IntCounter",
    "Test Statistic Descriptor Int Counter.","TestUnit");
  statDescriptorArr[1] = factory->createIntGauge("IntGauge",
    "Test Statistic Descriptor Int Gauge.","TestUnit");
  statDescriptorArr[2] = factory->createLongCounter("LongCounter",
    "Test Statistic Descriptor Long Counter.","TestUnit");
  statDescriptorArr[3] = factory->createLongGauge("LongGauge",
    "Test Statistic Descriptor Long Gauge.","TestUnit");
  statDescriptorArr[4] = factory->createDoubleCounter("DoubleCounter",
    "Test Statistic Descriptor Double Counter.","TestUnit");
  statDescriptorArr[5] = factory->createDoubleGauge("DoubleGauge",
    "Test Statistic Descriptor Double Gauge.","TestUnit");
  //Create a StatisticsType
  StatisticsType* statsType = factory->createType("TestStatsType",
    "Statistics for Unit Test.",statDescriptorArr, 6);
  //Create Statistics of a given type
  Statistics* testStat =
    factory->createStatistics(statsType,"TestStatistics");
  //Statistics are created and registered. Set and increment individual values
  int statIdIntCounter = statsType->nameToId("IntCounter");
  testStat->setInt(statIdIntCounter, 10 );
  testStat->incInt(statIdIntCounter, 1 );
  int currentValue = testStat->getInt(statIdIntCounter);
}

int main(int argc, char* argv[])
{
  try {
    printf("\nCppCachingApi EXAMPLES: Starting...");
    CppCachingApi cp4;
    printf("\nCppCachingApi EXAMPLES: Starting server...");
    cp4.startServer();
    printf("\nCppCachingApi EXAMPLES: Running example 4.1...");
    cp4.example_4_1();
    printf("\nCppCachingApi EXAMPLES: Running example 4.2...");
    cp4.example_4_2();
    printf("\nCppCachingApi EXAMPLES: Running example 4.3...");
    cp4.example_4_3();
    CacheHelper::cleanUp(cp4.cachePtr);
    printf("\nCppCachingApi EXAMPLES: Running example 4.4...");
    cp4.example_4_1();
    cp4.example_4_2();
    cp4.example_4_4();
    CacheHelper::cleanUp(cp4.cachePtr);
    cp4.example_4_1();
    cp4.example_4_2();
    cp4.example_4_3();
    printf("\nCppCachingApi EXAMPLES: Running example 4.5...");
    cp4.example_4_5();
    printf("\nCppCachingApi EXAMPLES: Running example 4.6...");
    cp4.example_4_6();
    printf("\nCppCachingApi EXAMPLES: Running example 4.12...");
    cp4.example_4_12();
    CacheHelper::cleanUp(cp4.cachePtr);
    printf("\nCppCachingApi EXAMPLES: stopping server...");
    cp4.stopServer();
    printf("\nCppCachingApi EXAMPLES: All Done.");
  }catch (const Exception & excp)
  {
    printf("\nEXAMPLES: %s: %s", excp.getName(), excp.getMessage());
    exit(1);
  }
  catch(...)
  {
    printf("\nEXAMPLES: Unknown exception");
    exit(1);
  }
  return 0;
}
