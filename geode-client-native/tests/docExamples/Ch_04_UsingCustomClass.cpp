/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//Example 4.11 Using a BankAccount Object

#include <gfcpp/GemfireCppCache.hpp>
#include "CacheHelper.hpp"
#include "BankAccount.hpp"
#include "AccountHistory.hpp"

using namespace gemfire;
using namespace docExample;

/**
* @brief Example 4.11 Using a BankAccount Object.
* This example creates the cache, registers types, creates a
* region, and then puts and gets user defined type BankAccount.
*/
int main( int argc, char** argv )
{
  // Start Server
  CacheHelper::initServer( 1, "UsingCustomClass.xml" );
  // Register the user-defined serializable type.
  Serializable::registerType( AccountHistory::createDeserializable );
  Serializable::registerType( BankAccount::createDeserializable );

  CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
  // Create a cache.
  CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)
      ->addServer("localhost", 24680)
      ->create();

  // Create a region.
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  RegionPtr  regionPtr = regionFactory->create("BankAccounts");

  // Place some instances of BankAccount cache region.
  BankAccountPtr KeyPtr(new BankAccount(2309, 123091));
  AccountHistoryPtr ValPtr(new AccountHistory());
  ValPtr->addLog( "Created account" );
  regionPtr->put( KeyPtr, ValPtr );
  printf( "Put an AccountHistory in cache keyed with BankAccount.\n" );
  // Call custom behavior on instance of BankAccount.
  KeyPtr->showAccountIdentifier();
  // Call custom behavior on instance of AccountHistory.
  ValPtr->showAccountHistory();
  // Get a value out of the region.
  AccountHistoryPtr historyPtr =
    dynCast<AccountHistoryPtr>( regionPtr->get( KeyPtr ) );
  if ( historyPtr != NULLPTR ) {
    printf( "Found AccountHistory in the cache.\n" );
    historyPtr->showAccountHistory();
    historyPtr->addLog( "debit $1,000,000." );
    regionPtr->put( KeyPtr, historyPtr );
    printf( "Updated AccountHistory in the cache.\n" );
  }
  // Look up the history again.
  historyPtr = dynCast<AccountHistoryPtr>( regionPtr->get( KeyPtr ) );
  if ( historyPtr != NULLPTR ) {
    printf( "Found AccountHistory in the cache.\n" );
    historyPtr->showAccountHistory();
  }
  // Close the cache.
  cachePtr->close();
  return 0;
}
