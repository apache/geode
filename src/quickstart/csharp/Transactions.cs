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

/*
 * The Transaction QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1.  Create a Geode Cache.
 * 2.  Create the example Region Programmatically.
 * 3   Begin Transaction
 * 4.  Put Entries (Key and Value pairs) into the Region.
 * 5.  Commit Transaction
 * 6.  Get Entries from the Region.
 * 7.  Begin Transaction
 * 8.  Put Entries (Key and Value pairs) into the Region.
 * 9.  Destroy key
 * 10. Rollback transaction
 * 11. Get Entries from the Region.
 * 12. Close the Cache.
 *
 */
 
// Use standard namespaces
using System;

// Use the Geode namespace
using Apache.Geode.Client;

// Use the .NET generics namespace
using System.Collections.Generic;

namespace Apache.Geode.Client.QuickStart
{

  // Cache Transactions QuickStart example.
  class Transactions
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a Geode Cache
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
        
        Cache cache = cacheFactory.Create();
        
        Console.WriteLine("Created the Geode cache.");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        Console.WriteLine("Created the RegionFactory.");

        // Create the example Region
        IRegion<string, string> region = regionFactory.Create<string, string>("exampleRegion");

        Console.WriteLine("Created the region with generics support.");

        // Get the cache transaction manager from the cache.
        CacheTransactionManager txManager = cache.CacheTransactionManager;

        // Starting a transaction
        txManager.Begin();
        Console.WriteLine("Transaction started.");
        
        region["Key1"] = "Value1";
        region["Key2"] = "Value2";
        
        Console.WriteLine("Put two entries into the region");
        
        try {
          txManager.Commit();
        }
        catch (CommitConflictException e)
        {
          Console.WriteLine("CommitConflictException encountered. Exception: {0}", e.Message);
        }
        
        if(region.ContainsKey("Key1"))
          Console.WriteLine("Obtained the first entry from the Region");
    
        if(region.ContainsKey("Key2"))
          Console.WriteLine("Obtained the second entry from the Region");
    
        //start a new transaction
        txManager.Begin();
        Console.WriteLine("Transaction Started");

        // Put a new entry 
        region["Key3"] = "Value3";
        Console.WriteLine("Put the third entry into the Region");

        // remove the first key
        region.Remove("Key1", null);
        Console.WriteLine("remove the first entry");
        
        txManager.Rollback();
        Console.WriteLine("Transaction Rollbacked");
    
        if(region.ContainsKey("Key1"))
          Console.WriteLine("Obtained the first entry from the Region");
    
        if(region.ContainsKey("Key2"))
          Console.WriteLine("Obtained the second entry from the Region");
        
        if(region.ContainsKey("Key3"))
          Console.WriteLine("ERROR: Obtained the third entry from the Region.");
        
        cache.Close();

        Console.WriteLine("Closed the Geode Cache");
      }
      // An exception should not occur
      catch (GeodeException gfex)
      {
        Console.WriteLine("Transactions Geode Exception: {0}", gfex.Message);
      }
    }
  }
}
