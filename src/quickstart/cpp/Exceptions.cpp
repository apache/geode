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
 * The Exceptions QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create CacheFactory using the user specified settings or from the
 * gfcpp.properties file by default.
 * 2. Create a Geode Cache.
 * 3. Get the example Regions from the Cache.
 * 4. Perform some operations which should cause exceptions.
 * 5. Close the Cache.
 * 6. Put an Entry into the Region when Cache is already closed.
 *
 */

// Include the Geode library.
#include <gfcpp/GeodeCppCache.hpp>

// Use the "geode" namespace.
using namespace apache::geode::client;

// The Exceptions QuickStart example.
int main(int argc, char** argv) {
  try {
    // Create CacheFactory using the user specified settings or from the
    // gfcpp.properties file by default.
    PropertiesPtr prp = Properties::create();
    prp->insert("cache-xml-file", "XMLs/clientExceptions.xml");

    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(prp);

    LOGINFO("Created CacheFactory");

    // Create a Geode Cache with the "clientExceptions.xml" Cache XML file.
    CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)->create();

    LOGINFO("Created the Geode Cache");

    // Get the example Regions from the Cache which are declared in the Cache
    // XML file.
    RegionPtr regionPtr = cachePtr->getRegion("exampleRegion");
    RegionPtr regionPtr2 = cachePtr->getRegion("exampleRegion2");

    LOGINFO("Obtained the Regions from the Cache");

    // Put an Entry (Key and Value pair) into the Region using the
    // direct/shortcut method.
    regionPtr->put("Key1", "Value1");

    LOGINFO("Put the first Entry into the Region");

    // Put an Entry into the Region by manually creating a Key and a Value pair.
    CacheableKeyPtr keyPtr = CacheableInt32::create(123);
    CacheablePtr valuePtr = CacheableString::create("123");
    regionPtr->put(keyPtr, valuePtr);

    LOGINFO("Put the second Entry into the Region");

    // Get Entries back out of the Region.
    CacheablePtr result1Ptr = regionPtr->get("Key1");

    LOGINFO("Obtained the first Entry from the Region");

    CacheablePtr result2Ptr = regionPtr->get(keyPtr);

    LOGINFO("Obtained the second Entry from the Region");

    // Destroy exampleRegion2.
    UserDataPtr userDataPtr = NULLPTR;
    regionPtr2->destroyRegion(userDataPtr);

    try {
      // Try to Put an Entry into a destroyed Region.
      regionPtr2->put("Key3", "Value3");

      LOGINFO("UNEXPECTED: Put should not have succeeded");

      return 1;
    } catch (const RegionDestroyedException& geodeExcp) {
      LOGINFO("Expected RegionDestroyedException: %s", geodeExcp.getMessage());
    }

    try {
      // Its not valid to create two instances of Cache with different settings.
      // If the settings are the same it returns the existing Cache instance.
      CacheFactoryPtr cacheFactory2 = CacheFactory::createCacheFactory(prp);
      CachePtr cachePtr1 = cacheFactory2->setSubscriptionEnabled(true)
                               ->addServer("localhost", 40405)
                               ->create();
      LOGINFO("UNEXPECTED: Cache create should not have succeeded");

      return 1;
    } catch (const IllegalStateException& geodeExcp) {
      LOGINFO("Expected IllegalStateException: %s", geodeExcp.getMessage());
    }

    // Close the Geode Cache.
    cachePtr->close();

    LOGINFO("Closed the Geode Cache");

    try {
      // Put an Entry into the Region when Cache is already closed.
      regionPtr->put("Key1", "Value1");

      LOGINFO("UNEXPECTED: Put should not have succeeded");

      return 1;
    } catch (const RegionDestroyedException& geodeExcp) {
      LOGINFO("Expected RegionDestroyedException: %s", geodeExcp.getMessage());
    }

  }
  // An exception should not occur
  catch (const Exception& geodeExcp) {
    LOGERROR("Exceptions Geode Exception: %s", geodeExcp.getMessage());

    return 1;
  }

  return 0;
}
