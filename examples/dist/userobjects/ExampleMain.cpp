/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

