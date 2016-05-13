/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_04_CacheableKeyBankAccount.hpp"

using namespace gemfire;
using namespace docExample;

ExampleBankAccountCacheable::ExampleBankAccountCacheable()
{
}

ExampleBankAccountCacheable::~ExampleBankAccountCacheable()
{
}


//start cacheserver
void ExampleBankAccountCacheable::startServer()
{
  CacheHelper::initServer( 1, "DataSerializationServer.xml" );
}

//stop cacheserver
void ExampleBankAccountCacheable::stopServer()
{
  CacheHelper::closeServer( 1 );
}

// Initialize Region
void ExampleBankAccountCacheable::initRegion()
{
  // Create a  region.
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr1 = regionFactory->setCachingEnabled(false)
      ->create("UserRegion");
}

int main(int argc, char* argv[])
{
  try {
    printf("\nExampleBankAccountCacheable EXAMPLES: Starting...");
    ExampleBankAccountCacheable ds;
    printf("\nExampleBankAccountCacheable EXAMPLES: Starting server...");
    ds.startServer();
    CacheHelper::connectToDs(ds.cacheFactoryPtr);
    printf("\nExampleBankAccountCacheable EXAMPLES: Init Cache...");
    CacheHelper::initCache(ds.cacheFactoryPtr, ds.cachePtr);
    printf("\nExampleBankAccountCacheable EXAMPLES: get Region...");
    ds.initRegion();

    Serializable::registerType( BankAccount::createInstance);

    BankAccountPtr bkptr(new BankAccount(10,100));
    ds.regionPtr1->put(10, bkptr);
    printf("\nExampleBankAccountCacheable EXAMPLES: BankAccount put success...");
    BankAccountPtr getbkPtr =
      dynCast<BankAccountPtr>( ds.regionPtr1->get( 10 ) );
    if ( getbkPtr != NULLPTR ) {
      printf( "\nFound BankAccount in the cache.\n" );
    }

    printf("\nExampleBankAccountCacheable EXAMPLES: cleanup...");
    CacheHelper::cleanUp(ds.cachePtr);
    printf("\nExampleBankAccountCacheable EXAMPLES: stopping server...");
    ds.stopServer();

    printf("\nExampleBankAccountCacheable EXAMPLES: All Done.");
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

