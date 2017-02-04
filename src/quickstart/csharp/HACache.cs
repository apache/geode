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
 * The HA QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Connect to a Geode Distributed System which has two cache servers.
 * 2. Create a Geode Cache with redundancy level = 1.
 * 3. Get the example generic Region from the Cache.
 * 4. Call registerKeys() on the Region.
 * 5. Call registerRegex() on the Region.
 * 6. Put two keys in the Region.
 * 7. Verify that the keys are destroyed via expiration in server. 
 * 8. Close the Cache.
 *
 */

// Use standard namespaces
using System;
using System.Threading;

// Use the Geode namespace
using Apache.Geode.Client;

namespace Apache.Geode.Client.QuickStart
{
  // The HA QuickStart example.
  class HA
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a Geode Cache.
        Apache.Geode.Client.CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientHACache.xml")
                  .AddServer("localhost", 40404)
                  .AddServer("localhost", 40405)
                  .SetSubscriptionRedundancy(1)
                  .SetSubscriptionEnabled(true)
                  .Create();

        Console.WriteLine("Created the Geode Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<object, int> region = cache.GetRegion<object, int>("/exampleRegion");

        Console.WriteLine("Obtained the generic Region from the Cache");

        // Register and Unregister Interest on Region for Some Keys.
        object [] keys = new object[] { 123, "Key-123" };
        region.GetSubscriptionService().RegisterKeys(keys);
        region.GetSubscriptionService().RegisterRegex("Keys.*");

        Console.WriteLine("Called RegisterKeys() and RegisterRegex()");

        region[123] = 1;
        region["Key-123"] = 2;
        
        Console.WriteLine("Called put() on Region");
        
        Console.WriteLine("Waiting for updates on keys");
        Thread.Sleep(10000);
        
        int count = 0;

        //try to get the entries for keys destroyed by server.
        try
        {
          int value1 = region[123];
          Console.WriteLine("UNEXPECTED: First get should not have succeeded");

        }
        catch(KeyNotFoundException){
          Console.WriteLine("gfex.Message: Verified that key1 has been destroyed");
          count++;
        }

        try
        {
          int value2 = region["Key-123"];
          Console.WriteLine("UNEXPECTED: Second get should not have succeeded");
        }
        catch (KeyNotFoundException)
        {
          Console.WriteLine("gfex.Message: Verified that key2 has been destroyed");
          count++;
        }

        if (count == 2) {
          Console.WriteLine("Verified all updates");
        }
        else {
          Console.WriteLine("Could not verify all updates");
        }

        region.GetSubscriptionService().UnregisterKeys(keys);
        region.GetSubscriptionService().UnregisterRegex("Keys.*");
    
        Console.WriteLine("Unregistered keys");
            
        // Close the Geode Cache.
        cache.Close();

        Console.WriteLine("Closed the Geode Cache");
      }
      // An exception should not occur
      catch (GeodeException gfex)
      {
        Console.WriteLine("HACache Geode Exception: {0}", gfex.Message);
      }
    }
  }
}
