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

using System;

namespace GemStone.GemFire.Cache.Examples
{
  class UserObjects
  {
    static void Main()
    {
      // Register the user-defined serializable type.
      Serializable.RegisterType(AccountHistory.CreateInstance);
      Serializable.RegisterType(BankAccount.CreateInstance);

      // Create a GemFire Cache Programmatically.
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(null);
      Cache cache = cacheFactory.SetSubscriptionEnabled(true).Create();

      Console.WriteLine("Created the GemFire Cache");

      RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.LOCAL);
      Console.WriteLine("Created Region Factory");

      // Create the example Region programmatically.
      Region region = regionFactory
        .Create("BankAccounts");

      Console.WriteLine("Created the Region Programmatically.");

      // Place some instances of BankAccount cache region.
      BankAccount baKey = new BankAccount(2309, 123091);
      AccountHistory ahVal = new AccountHistory();
      ahVal.AddLog("Created account");
      region.Put(baKey, ahVal);
      Console.WriteLine("Put an AccountHistory in cache keyed with BankAccount.");

      // Display the BankAccount information.
      Console.WriteLine(baKey.ToString());

      // Call custom behavior on instance of AccountHistory.
      ahVal.ShowAccountHistory();

      // Get a value out of the region.
      AccountHistory history = region.Get(baKey) as AccountHistory;
      if (history != null)
      {
        Console.WriteLine("Found AccountHistory in the cache.");
        history.ShowAccountHistory();
        history.AddLog("debit $1,000,000.");
        region.Put(baKey, history);
        Console.WriteLine("Updated AccountHistory in the cache.");
      }

      // Look up the history again.
      history = region.Get(baKey) as AccountHistory;
      if (history != null)
      {
        Console.WriteLine("Found AccountHistory in the cache.");
        history.ShowAccountHistory();
      }

      // Close the cache.
      cache.Close();

    }
  }
}
