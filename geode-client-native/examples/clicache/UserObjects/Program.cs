/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
