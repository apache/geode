/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "Ch_04_SerializableBankAccount.hpp"
#include "CacheHelper.hpp"

using namespace gemfire;
using namespace docExample;

//constructor
ExampleBankAccountSerialization::ExampleBankAccountSerialization()
{
}

//destructor
ExampleBankAccountSerialization::~ExampleBankAccountSerialization()
{
}

//start cacheserver
void ExampleBankAccountSerialization::startServer()
{
  CacheHelper::initServer( 1, "DataSerializationServer.xml" );
}

//stop cacheserver
void ExampleBankAccountSerialization::stopServer()
{
  CacheHelper::closeServer( 1 );
}

void ExampleBankAccountSerialization::initRegion()
{
  // Create a  region.
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr1
      = regionFactory->create("UserRegion");
}

int main(int argc, char* argv[])
{
  try {
    printf("\nExampleBankAccountSerialization EXAMPLES: Starting...");
    ExampleBankAccountSerialization ds;
    printf("\nExampleBankAccountSerialization EXAMPLES: Starting server...");
    ds.startServer();
    printf("\nExampleBankAccountSerialization EXAMPLES: connect to DS...");
    CacheHelper::connectToDs(ds.cacheFactoryPtr);
    printf("\nExampleBankAccountSerialization EXAMPLES: Init Cache...");
    CacheHelper::initCache(ds.cacheFactoryPtr, ds.cachePtr);
    printf("\nExampleBankAccountSerialization EXAMPLES: get Region...");
    ds.initRegion();

    Serializable::registerType( BankAccount::createInstance);

    BankAccountPtr bkptr(new BankAccount(10,100));
    ds.regionPtr1->put(10, bkptr);
    printf("\nExampleBankAccountSerialization EXAMPLES: BankAccount put success...");
    BankAccountPtr getbkPtr =
      dynCast<BankAccountPtr>( ds.regionPtr1->get( 10 ) );
    if ( getbkPtr != NULLPTR ) {
      printf( "\nFound BankAccount in the cache.\n" );
    }

    printf("\nExampleBankAccountSerialization EXAMPLES: cleanup...");
    CacheHelper::cleanUp(ds.cachePtr);
    printf("\nExampleBankAccountSerialization EXAMPLES: stopping server...");
    ds.stopServer();

    printf("\nExampleBankAccountSerialization EXAMPLES: All Done.");
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
