/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 * 
 * This example demonstrates client/server caching. This is a classic
 * client-server architecture (versus peer-to-peer). This program starts a client 
 * that connects to a server so it can request, update, and delete data. 
 * 
 * While this example uses a console application, it is not a requirement.
 * 
 * Please note that this example requires that the GemFire HierarchicalServer
 * process to be running prior to execution.  To start the HierarchicalServer
 * QuickStart example, please refer to the GemFire Quickstart documentation.
 *
//////////////////////////////////////////////////////////////////////////////*/
using System;

// These namespaces provide access to classes needed to interact with GemFire.
using GemStone.GemFire.Cache;

public class HierarchicalClient
{
  public static void Main()
  {
    DistributedSystem MyDistributedSystem = null;
    Cache MyCache = null;
    string sKey = null;
    CacheableString sValue = null;

    try
    {
      Console.WriteLine("* Connecting to the distributed system and creating the cache.");

      /* Properties can be passed to GemFire through two different mechanisms: the 
       * Properties object as is done below or the gemfire.properties file. The 
       * settings passed in a Properties object take precedence over any settings
       * in a file. This way the end-user cannot bypass any required settings.
       * 
       * Using a gemfire.properties file can be useful when you want to change the 
       * behavior of certain settings without doing a new build, test, and deploy cycle.
       * 
       * See gemfire.properties for details on some of the other settings used in this 
       * project.
       * 
       * For details on all of the possible settings and their respective values 
       * that can be specified in this configuration file, see chapter 5, 
       * "System Configuration", in the "System Administrator's Guide". This is
       * installed in the docs\pdf folder under the GemFire installation folder
       * (e.g., C:\Program Files\Gemfire5\docs\pdf\SystemAdmin.pdf).
      */
      Properties DistributedSystemProperties = new Properties();

      DistributedSystemProperties.Insert("name", "CacheClient");

      /* Specify the file whose contents are used to initialize the cache when it is created.
       * 
       * An XML file isn't needed at all because everything can be specified in  code--much
       * as the "license-file" property is. However, it provides a convenient way
       * to isolate common settings that can be updated without a build/test/deploy cycle.
      */
      DistributedSystemProperties.Insert("cache-xml-file", "HierarchicalClient.xml");

      /* Define where the license file is located. It is very useful to do this in
       * code vs. the gemfire.properties file, because it allows you to access the
       * license used by the GemFire installation (as pointed to by the GEMFIRE
       * environment variable).
      */
      DistributedSystemProperties.Insert("license-file", "../../gfCppLicense.zip");

      DistributedSystemProperties.Insert("log-file", "./csharpclient.log");
      DistributedSystemProperties.Insert("log-level", "finest");

      /* Override the mcast-port setting so the client runs "standalone".
         The client and server must run in separate distributed systems.
      */
      //DistributedSystemProperties.Insert("mcast-port", "0");

      // Connect to the GemFire distributed system.
      MyDistributedSystem = DistributedSystem.Connect("LocalDS", DistributedSystemProperties);

      /*//////////////////////////////////////////////////////////////////////////////
       * 
       * Create the cache.
       * 
      //////////////////////////////////////////////////////////////////////////////*/

      // Create the cache. This causes the cache-xml-file to be parsed.
      MyCache = CacheFactory.Create("localCache", MyDistributedSystem);

      /*//////////////////////////////////////////////////////////////////////////////
       * 
       * Create the region.
       * 
      //////////////////////////////////////////////////////////////////////////////*/

      // Prepare the attributes needed to create a sub-region.
      AttributesFactory MyAttributesFactory = new AttributesFactory();

      /* The "scope" determines how changes to the local cache are "broadcast"
       * to like-named regions in other caches.
       * 
       * For native clients DistributedAck and DistributedNoAck work
       * identically.
      */
      MyAttributesFactory.SetScope(ScopeType.DistributedAck);

      /* Endpoints is a  comma delimited list of logical names, hostnames, and ports of 
       * "server" caches with which to connect. The endpoints parameter follows this syntax: 
       * 
       *      logicalName1=host1:port1, . . . ,logicalNameN=hostN:portN
      */
      MyAttributesFactory.SetEndpoints("localhost:40404");
      MyAttributesFactory.SetClientNotificationEnabled(true);

      /* Because of implementation details, it is best not to cache data in a root-level 
       * region. There is nothing special about the name "root", it is just a good naming
       * convention.
       * 
       * Get the "root" region from the cache and create a sub-region under it for the
       * data.
      */
      Region MyExampleRegion = MyCache.GetRegion("root").CreateSubRegion(
        "exampleRegion", MyAttributesFactory.CreateRegionAttributes());

      Console.WriteLine(String.Format("{0}* Region, {1}, was created in the cache.",
        Environment.NewLine, MyExampleRegion.FullPath));
      Console.WriteLine("* Getting three values from the Hierarchical Server.");

      // Retrieve several values from the cache.
      for (int nCount = 0; nCount < 4; nCount++)
      {
        sKey = string.Format("Key{0}", nCount);

        Console.WriteLine(String.Format("* Requesting object: {0}{1}",
          sKey, Environment.NewLine));

        /* Request the object from the cache. Because it doesn't exist in the local
         * cache, it will be passed to the server. Because the server doesn't have
         * it, the request will be passed to SimpleCacheLoader.Load().  The entry 
         * returned is a string.
        */
        sValue = MyExampleRegion.Get(sKey) as CacheableString;

        Console.WriteLine(String.Format("* Retrieved object: ({0})", sValue.ToString()));
      }

      Console.WriteLine("* If you look at the Cache Server's console, you can see the CacheListener notifications that happened in response to the gets.");
      Console.WriteLine("{0}---[ Press <Enter> to continue. ]---", Environment.NewLine);


      Console.ReadLine();

      /*//////////////////////////////////////////////////////////////////////////////
       * 
       * Exercise the serialization and deserialization methods.
       * 
      //////////////////////////////////////////////////////////////////////////////*/

      // Demonstrate the process needed to manually deserialize the object from the cache.
      Console.WriteLine(string.Format("* Manually deserializing the object for Key0: ({0})", MyExampleRegion.Get("Key0")));

      // Demonstrate the static FromCache method in the CachedItem class and modify the object.
      Console.WriteLine("* Using the FromCache() method and modifying the object for Key0");

      // Get the item.
      //sValue = (CacheableString)MyExampleRegion.Get("Key0");
      sValue = MyExampleRegion.Get("Key0") as CacheableString;

      Console.WriteLine(string.Format("* Original value: ({0})", sValue.ToString()));

      /* This modifies the object associated with Key0 and uses CacheSerializer
       * to perform manual serialization.
      */
      String cachedItem = "PDA";
      MyExampleRegion.Put("Key0", cachedItem);

      // Reread the object from the cache.
      sValue = (CacheableString)MyExampleRegion.Get("Key0");

      Console.WriteLine(string.Format("* Retrieved updated object: {0}", sValue));

      Console.WriteLine("* Invalidating the data for Key2");

      /* Invalidating a cached item causes the object to be removed, but the 
       * key remains in the cache. If it is subsequently requested it will
       * be retrieved using a CacheLoader if possible. 
      */
      MyExampleRegion.Invalidate("Key2");

      Console.WriteLine("* Requesting Key2 after the invalidation.");

      // Request the invalidated item.
      sValue = (CacheableString)MyExampleRegion.Get("Key2");

      Console.WriteLine(string.Format("* Retrieved object: {0}", sValue));

      Console.WriteLine("* Destroying Key3");

      // Destroying a cached item removes both the object and the key.
      MyExampleRegion.Destroy("Key3");

      Console.WriteLine("{0}---[ Press <Enter> to End the Application ]---",
        Environment.NewLine);
      Console.ReadLine();
    }
    catch (Exception ThrownException)
    {
      Console.Error.WriteLine(ThrownException.Message);
      Console.Error.WriteLine(ThrownException.StackTrace);
      Console.Error.WriteLine("---[ Press <Enter> to End the Application ]---");
      Console.ReadLine();
    }
    finally
    {
      /* While there are not any ramifications of terminating without closing the cache
       * and disconnecting from the distributed system, it is considered a best practice
       * to do so.
      */
      try
      {
        Console.WriteLine("Closing the cache and disconnecting.{0}",
          Environment.NewLine);
      }
      catch {/* Ignore any exceptions */}

      try
      {
        /* Close the cache. This terminates the cache and releases all the resources. 
         * Generally speaking, after a cache is closed, any further method calls on 
         * it or region object will throw an exception.
        */
        MyCache.Close();
      }
      catch {/* Ignore any exceptions */}

      try
      {
        /* Disconnect from the distributed system.                
        */
        MyDistributedSystem = null;
      }
      catch {/* Ignore any exceptions */}
    }
  }

  public static string GetStr(string key, Region region)
  {
    IGFSerializable cValue = region.Get(key);

    Console.WriteLine("    Get string  -- key: " + key + "   value: " + cValue.ToString());
    return cValue.ToString();
  }
}
