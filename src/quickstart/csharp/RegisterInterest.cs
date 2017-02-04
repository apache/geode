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
 * The RegisterInterest QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create CacheFactory using the user specified properties or from the gfcpp.properties file by default.
 * 2. Create a Geode Cache.
 * 3. Get the example Region from the Cache.
 * 4. Call registerAllKeys() and unregisterAllKeys() on the Region.
 * 5. Call registerKeys() and unregisterKeys() on the Region.
 * 6. Call registerRegex() and unregisterRegex() on the Region.
 * 7. Close the Cache.
 *
 */
// Use standard namespaces
using System;
using System.Collections.Generic;

// Use the Geode namespace
using Apache.Geode.Client;

namespace Apache.Geode.Client.QuickStart
{
  // The RegisterInterest QuickStart example.
  class RegisterInterest
  {
    static void Main(string[] args)
    {
      try
      {
        //Create CacheFactory using the user specified properties or from the gfcpp.properties file by default.
        Properties<string, string> prp = Properties<string, string>.Create<string, string>();
        prp.Insert("cache-xml-file", "XMLs/clientRegisterInterest.xml");
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prp);

        Console.WriteLine("Created CacheFactory");

        // Create a Geode Cache with the "clientRegisterInterest.xml" Cache XML file.
        Cache cache = cacheFactory.Create();

        Console.WriteLine("Created the Geode Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<string, string> region = cache.GetRegion<string, string>("exampleRegion");

        Console.WriteLine("Obtained the Region from the Cache");

        // Register and Unregister Interest on Region for All Keys.
        region.GetSubscriptionService().RegisterAllKeys();
        region.GetSubscriptionService().UnregisterAllKeys();

        Console.WriteLine("Called RegisterAllKeys() and UnregisterAllKeys()");

        // Register and Unregister Interest on Region for Some Keys.
        ICollection<string> keys = new List<string>();
        keys.Add("123");
        keys.Add("Key-123");

        region.GetSubscriptionService().RegisterKeys(keys);
        region.GetSubscriptionService().UnregisterKeys(keys);

        Console.WriteLine("Called RegisterKeys() and UnregisterKeys()");

        // Register and Unregister Interest on Region for Keys matching a Regular Expression.
        region.GetSubscriptionService().RegisterRegex("Keys-*");
        region.GetSubscriptionService().UnregisterRegex("Keys-*");

        Console.WriteLine("Called RegisterRegex() and UnregisterRegex()");

        //Register Interest on Region for All Keys with getInitialValues to populate the cache with values of all keys from the server.
        region.GetSubscriptionService().RegisterAllKeys(false, null, true); // Where the 3rd argument is getInitialValues.
        //Unregister Interest on Region for All Keys.
        region.GetSubscriptionService().UnregisterAllKeys();

        Console.WriteLine("Called RegisterAllKeys() and UnregisterAllKeys() with getInitialValues argument");
    
        //Register Interest on Region for Some Keys with getInitialValues.
        region.GetSubscriptionService().RegisterKeys(keys, false, true); // Where the 3rd argument is getInitialValues.

        //Unregister Interest on Region for Some Keys.
        region.GetSubscriptionService().UnregisterKeys(keys);

        Console.WriteLine("Called RegisterKeys() and UnregisterKeys() with getInitialValues argument");
        
        //Register and Unregister Interest on Region for Keys matching a Regular Expression with getInitialValues.
        region.GetSubscriptionService().RegisterRegex("Keys-*", false, null, true);
        region.GetSubscriptionService().UnregisterRegex("Keys-*");

        Console.WriteLine("Called RegisterRegex() and UnregisterRegex() with getInitialValues argument");
        
        // Close the Geode Cache.
        cache.Close();

        Console.WriteLine("Closed the Geode Cache");
      }
      // An exception should not occur
      catch (GeodeException gfex)
      {
        Console.WriteLine("RegisterInterest Geode Exception: {0}", gfex.Message);
      }
    }
  }
}
