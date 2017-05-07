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
