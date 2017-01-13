
#include <gfcpp/GemfireCppCache.hpp>
#include "BankAccount.hpp"
#include "AccountHistory.hpp"

using namespace gemfire;

/*
  This example registers types, creates the cache, creates a
  region, and then puts and gets user defined type BankAccount.
*/
int main( int argc, char** argv )
{
  // Register the user defined, serializable type.
  Serializable::registerType( AccountHistory::createDeserializable );
  Serializable::registerType( BankAccount::createDeserializable );

  // Create a cache.
  CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
  CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)->create();   

  LOGINFO("Created the GemFire Cache");
  
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(LOCAL);
  
  LOGINFO("Created the RegionFactory");

  // Create the Region programmatically.
  RegionPtr regionPtr = regionFactory->create("BankAccounts");

  LOGINFO("Created the Region from the Cache");

  // Place some instances of BankAccount cache region.
  BankAccountPtr baKeyPtr(new BankAccount(2309, 123091));
  AccountHistoryPtr ahValPtr(new AccountHistory());
  ahValPtr->addLog( "Created account" );

  regionPtr->put( baKeyPtr, ahValPtr );
  printf( "Put an AccountHistory in cache keyed with BankAccount.\n" );
  // Call custom behavior on instance of BankAccount.
  baKeyPtr->showAccountIdentifier();
  // Call custom behavior on instance of AccountHistory.
  ahValPtr->showAccountHistory();

  // Get a value out of the region.
  AccountHistoryPtr historyPtr = dynCast<AccountHistoryPtr>( regionPtr->get( baKeyPtr ) );
  if (historyPtr != NULLPTR) {
    printf( "Found AccountHistory in the cache.\n" );
    historyPtr->showAccountHistory();

    historyPtr->addLog( "debit $1,000,000." );
    regionPtr->put( baKeyPtr, historyPtr );
    printf( "Updated AccountHistory in the cache.\n" );
  }

  // Look up the history again.
  historyPtr = dynCast<AccountHistoryPtr>( regionPtr->get( baKeyPtr ) );
  if (historyPtr != NULLPTR) {
    printf( "Found AccountHistory in the cache.\n" );
    historyPtr->showAccountHistory();
  }

  // Close the cache.
  cachePtr->close();

  return 0;
}

