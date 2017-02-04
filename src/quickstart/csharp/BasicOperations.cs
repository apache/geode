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
 * The BasicOperations QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a Geode Cache.
 * 2. Create the example Region with generics support programmatically.
 * 3.a. Put Entries (Key and Value pairs) into the Region.
 * 3.b. If in 64 bit mode put over 4 GB data to demonstrate capacity.
 * 4. Get Entries from the Region.
 * 5. Invalidate an Entry in the Region.
 * 6. Destroy an Entry in the Region.
 * 7. Close the Cache.
 *
 */

// Use standard namespaces
using System;

// Use the Geode namespace
using Apache.Geode.Client;

// To check for available memory.
using System.Diagnostics;

namespace Apache.Geode.Client.QuickStart
{

  // The BasicOperations QuickStart example.
  class BasicOperations
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a Geode Cache.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Cache cache = cacheFactory.Create();

        Console.WriteLine("Created the Geode Cache");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        IRegion<int, string> region = regionFactory.Create<int, string>("exampleRegion");

        Console.WriteLine("Created the Region with generics support programmatically.");

        // Put an Entry (Key and Value pair) into the Region using the IDictionary interface. 
        region[111] = "Value1";

        Console.WriteLine("Put the first Entry into the Region");

        // Put an Entry into the Region the traditional way. 
        region[123] = "123";

        Console.WriteLine("Put the second Entry into the Region");

        PerformanceCounter pc = new PerformanceCounter("Memory", "Available Bytes");
        long freeMemory = Convert.ToInt64(pc.NextValue());

        // Are we a 64 bit process and do we have 5 GB free memory available? 
        if (IntPtr.Size == 8 && freeMemory > 5L * 1024L * 1024L * 1024L)
        {
          Char ch = 'A';
          string text = new string(ch, 1024 * 1024);
          Console.WriteLine("Putting over 4 GB data locally...");
          for (int item = 0; item < (5 * 1024 /* 5 GB */); item++)
          {
            region.GetLocalView()[item] = text;
          }
          Console.WriteLine("Put over 4 GB data locally");
        }
        else
        {
          Console.WriteLine("Not putting over 4 GB data locally due to lack of memory capacity");
        }
        
        // Get Entries back out of the Region via the IDictionary interface.
        string result1 = region[111];

        Console.WriteLine("Obtained the first Entry from the Region");

        string result2 = region[123];

        Console.WriteLine("Obtained the second Entry from the Region");

        // Invalidate an Entry in the Region.
        region.Invalidate(111);

        Console.WriteLine("Invalidated the first Entry in the Region");

        // Destroy an Entry in the Region using the IDictionary interface.
        region.Remove(123);

        Console.WriteLine("Destroyed the second Entry in the Region");

        // Close the Geode Cache.
        cache.Close();

        Console.WriteLine("Closed the Geode Cache");
      }
      // An exception should not occur
      catch (GeodeException gfex)
      {
        Console.WriteLine("BasicOperations Geode Exception: {0}", gfex.Message);
      }
    }
  }
}
