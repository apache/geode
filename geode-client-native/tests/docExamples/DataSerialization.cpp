/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "DataSerialization.hpp"
#include "Ch_13_User.hpp"
#include "Ch_13_ExampleObject.hpp"

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace docExample;

//constructor
DataSerialization::DataSerialization()
{
}

//destructor
DataSerialization::~DataSerialization()
{
}

//start cacheserver
void DataSerialization::startServer()
{
  CacheHelper::initServer( 1, "DataSerializationServer.xml" );
}

//stop cacheserver
void DataSerialization::stopServer()
{
  CacheHelper::closeServer( 1 );
}

void DataSerialization::initRegion()
{
  // Create regions.
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr1 = regionFactory->setCachingEnabled(
          true)->create("UserRegion");
  regionPtr2 = regionFactory->setCachingEnabled(
          true)->create("ExampleRegion");
}

int main(int argc, char* argv[])
{
  try {
    printf("\nDataSerialization EXAMPLES: Starting...");
    DataSerialization ds;
    printf("\nDataSerialization EXAMPLES: Starting server...");
    ds.startServer();
    CacheHelper::connectToDs(ds.cacheFactoryPtr);
    printf("\nDataSerialization EXAMPLES: Init Cache...");
    CacheHelper::initCache(ds.cacheFactoryPtr, ds.cachePtr);
    printf("\nDataSerialization EXAMPLES: get Region...");
    ds.initRegion();

    Serializable::registerType( User::createInstance);
    Serializable::registerType( ExampleObject::createInstance);

    const char* names = "name";
    char ch = 'c';
    UserPtr userptr(new User(names, ch ));
    ds.regionPtr1->put(10, userptr);
    printf("\nDataSerialization EXAMPLES: User put success...");
    ds.regionPtr1->get(10);
    printf("\nDataSerialization EXAMPLES: User get success...");

    ExampleObjectPtr exptr(new ExampleObject(100));
    ds.regionPtr2->put(10,exptr);
    printf("\nDataSerialization EXAMPLES: Example put success...");
    ds.regionPtr2->get(10);
    printf("\nDataSerialization EXAMPLES: Example get success...");

    printf("\nDataSerialization EXAMPLES: cleanup...");
    CacheHelper::cleanUp(ds.cachePtr);
    printf("\nDataSerialization EXAMPLES: stopping server...");
    ds.stopServer();

    printf("\nDataSerialization EXAMPLES: All Done.");
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
