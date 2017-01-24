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
 * 1. Sets the client security properties.
 * 2. Put an Entry ( for which it has valid credentials ).
 * 3. Fail to Get an Entry ( for which user doesn't have permission ).
 * 4. Close the Cache.
 *
 */

// Include the GemFire library.
#include <gfcpp/GeodeCppCache.hpp>

// Use the "gemfire" namespace.
using namespace apache::geode::client;

// The Security QuickStart example.
int main(int argc, char** argv) {
  try {
    // Create client's Authentication Intializer and Credentials using api (
    // Same can be set to gfcpp.properties & comment following code ).
    PropertiesPtr properties = Properties::create();
    properties->insert("security-client-auth-factory",
                       "createPKCSAuthInitInstance");
    properties->insert("security-client-auth-library", "securityImpl");
    properties->insert("security-keystorepath", "keystore/gemfire6.keystore");
    properties->insert("security-alias", "gemfire6");
    properties->insert("security-keystorepass", "gemfire");
    properties->insert("cache-xml-file", "XMLs/clientSecurity.xml");

    // overriding secProp properties.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(properties);

    LOGINFO("Connected to the GemFire Distributed System");

    // Create a GemFire Cache with the "clientSecurity.xml" Cache XML file.
    CachePtr cachePtr = cacheFactory->create();

    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML
    // file.
    RegionPtr regionPtr = cachePtr->getRegion("exampleRegion");

    LOGINFO("Obtained the Region from the Cache");

    // Put an Entry (Key and Value pair) into the Region using the
    // direct/shortcut method.
    regionPtr->put("Key1", "Value1");

    LOGINFO("Entry created in the Region");

    try {
      // Get Entries back out of the Region.
      CacheablePtr result1Ptr = regionPtr->get("Key1");

      // collect NotAuthorized exception
    } catch (const apache::geode::client::NotAuthorizedException& expected) {
      LOGINFO(
          "Got expected authorization failure while obtaining the Entry: %s",
          expected.getMessage());
    }

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

    return 0;
  }
  // An exception should not occur
  catch (const Exception& gemfireExcp) {
    LOGERROR("Security GemFire Exception: %s", gemfireExcp.getMessage());

    return 1;
  }
}
