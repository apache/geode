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
  class HelloWorld
  {

    #region Local constants

    //private static Cache cache = null;
    private static Region region = null;

    private static string strKey = "abc";
    private static string strValue = "Hello, World!";

    private static int intKey = 777;
    private static int intValue = 12345678;

    #endregion

    static void Main()
    {
      try
      {
        // Create a GemFire Cache.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(null);
        Cache cache = cacheFactory.Create();

        Console.WriteLine("Created the GemFire Cache");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.LOCAL);

        // Create the example Region programmatically.
        region = regionFactory.Create("exampledsregion");

        // Put some entries
        Console.WriteLine("{0}Putting entries", Environment.NewLine);
        PutStr(strKey, strValue);
        PutInt(intKey, intValue);

        // Get the entries
        Console.WriteLine("{0}Getting entries", Environment.NewLine);
        string getStr = GetStr(strKey);
        int getInt = GetInt(intKey);

        // Close cache 
        cache.Close();
        Console.WriteLine("Closed the cache.");
       
      }
      catch (Exception ex)
      {
        Console.WriteLine("{0}An exception occurred: {1}", Environment.NewLine, ex);
        Console.WriteLine("---[ Press <Enter> to End the Application ]---");
        Console.ReadLine();
      }
    }

    #region Local functions

    public static void PutInt(int key, int value)
    {
      region.Put(key, value);
      Console.WriteLine("    Put integer -- key: " + key + "   value: " + value);
    }

    public static int GetInt(int key)
    {
      CacheableInt32 cValue = region.Get(key) as CacheableInt32;

      Console.WriteLine("    Get integer -- key: " + key + "   value: " + cValue.Value);
      return cValue.Value;
    }

    public static void PutStr(string key, string value)
    {
      region.Put(key, value);
      Console.WriteLine("    Put string  -- key: " + key + "   value: " + value);
    }

    public static string GetStr(string key)
    {
      IGFSerializable cValue = region.Get(key);

      Console.WriteLine("    Get string  -- key: " + key + "   value: " + cValue);
      return cValue.ToString();
    }

    #endregion
  }
}
