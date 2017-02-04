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
 * The Security QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Sets the authinit property and the other security properties.
 * 2. Connect to a Geode Distributed System.
 * 3. Does all operations. ( for which it has all permissions)
 * 4. Does a put and get. ( for which it have put permission. )
 * 5. Does a get and put. ( for which it have get permission. )
 * 5. Close the Cache with keepalive options as true.
 * 6. Disconnect from the Distributed System.
 *
 */

// Use standard namespaces
using System;

// Use the Geode namespace
using Apache.Geode.Client;

namespace Apache.Geode.Client.QuickStart
{
  // The Security QuickStart example.
  class SecurityExample
  {
    public void RunSecurityExampleWithPutPermission()
    {
      // Create client's Authentication Intializer and Credentials using api ( Same can be set to gfcpp.properties & comment following code ).
      Properties<string, string> properties = Properties<string, string>.Create<string, string>();
      properties.Insert("security-client-auth-factory", "Apache.Geode.Templates.Cache.Security.UserPasswordAuthInit.Create");
      properties.Insert("security-client-auth-library", "Apache.Geode.Templates.Cache.Security");
      properties.Insert("cache-xml-file", "XMLs/clientSecurity.xml");
      properties.Insert("security-username", "writer1");
      properties.Insert("security-password", "writer1");

      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(properties);

      Cache cache = cacheFactory.Create();

      Console.WriteLine("Created the Geode Cache");

      // Get the example Region from the Cache which is declared in the Cache XML file.
      IRegion<string, string> region = cache.GetRegion<string, string>("exampleRegion");

      Console.WriteLine("Obtained the Region from the Cache");

      region["key-3"] = "val-3";
      region["key-4"] = "val-4";

      bool exceptiongot = false;

      try
      {
        string getResult = region["key-3"];
      }
      catch (NotAuthorizedException ex)
      {
        Console.WriteLine("Got expected UnAuthorizedException: {0}", ex.Message);
        exceptiongot = true;
      }

      if (exceptiongot == false)
      {
        Console.WriteLine("Example FAILED: Did not get expected NotAuthorizedException");
      }
      cache.Close();
    }

    public void RunSecurityExampleWithGetPermission()
    {
      // Create client's Authentication Intializer and Credentials using api ( Same can be set to gfcpp.properties & comment following code ).
      Properties<string, string> properties = Properties<string, string>.Create<string, string>();
      properties.Insert("security-client-auth-factory", "Apache.Geode.Templates.Cache.Security.UserPasswordAuthInit.Create");
      properties.Insert("security-client-auth-library", "Apache.Geode.Templates.Cache.Security");
      properties.Insert("cache-xml-file", "XMLs/clientSecurity.xml");
      properties.Insert("security-username", "reader1");
      properties.Insert("security-password", "reader1");

      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(properties);

      Cache cache = cacheFactory.Create();

      Console.WriteLine("Created the Geode Cache");

      // Get the example Region from the Cache which is declared in the Cache XML file.
      IRegion<string, string> region = cache.GetRegion<string, string>("exampleRegion");

      Console.WriteLine("Obtained the Region from the Cache");

      string getResult1 = region["key-3"];
      string getResult2 = region["key-4"];

      bool exceptiongot = false;

      try
      {
        region["key-5"] = "val-5";
      }
      catch (NotAuthorizedException ex)
      {
        Console.WriteLine("Got expected UnAuthorizedException: {0}", ex.Message);
        exceptiongot = true;
      }

      if (exceptiongot == false)
      {
        Console.WriteLine("Example FAILED: Did not get expected NotAuthorizedException");
      }
      cache.Close();
    }

    public void RunSecurityExampleWithAllPermission()
    {
      // Create client's Authentication Intializer and Credentials using api ( Same can be set to gfcpp.properties & comment following code ).
      Properties<string, string> properties = Properties<string, string>.Create<string, string>();
      properties.Insert("security-client-auth-factory", "Apache.Geode.Templates.Cache.Security.UserPasswordAuthInit.Create");
      properties.Insert("security-client-auth-library", "Apache.Geode.Templates.Cache.Security");
      properties.Insert("cache-xml-file", "XMLs/clientSecurity.xml");
      properties.Insert("security-username", "root");
      properties.Insert("security-password", "root");

      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(properties);

      Cache cache = cacheFactory.Create();

      Console.WriteLine("Created the Geode Cache");

      // Get the example Region from the Cache which is declared in the Cache XML file.
      IRegion<string, string> region = cache.GetRegion<string, string>("exampleRegion");

      Console.WriteLine("Obtained the Region from the Cache");

      //put
      region["key-1"] = "val-1";
      region["key-2"] = "val-2";

      //get
      string getResult = region["key-1"];

      //invalidate key
      region.Invalidate("key-1");

      //Remove key
      region.Remove("key-2");


      //close caache
      cache.Close();
    }

    static void Main(string[] args)
    {
      try
      {
        SecurityExample ex = new SecurityExample();
        ex.RunSecurityExampleWithAllPermission();
        ex.RunSecurityExampleWithPutPermission();
        ex.RunSecurityExampleWithGetPermission();
      }
      // An exception should not occur
      catch (GeodeException gfex)
      {
        Console.WriteLine("SecurityExample Geode Exception: {0}", gfex.Message);
      }
    }
  }
}
