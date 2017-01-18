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
 * 1. Create a GemFire Cache with redundancy level = 1.
 * 2. Get the example Region from the Cache.
 * 3. Call registerKeys() on the Region.
 * 4. Call registerRegex() on the Region.
 * 5. Put two keys in the Region.
 * 6. Verify that the keys are destroyed via expiration in server.
 * 7. Close the Cache.
 *
 */

// Include the GemFire library.
#include <gfcpp/GeodeCppCache.hpp>

// Use the "gemfire" namespace.
using namespace apache::geode::client;

// The HA QuickStart example.
int main(int argc, char** argv) {
  try {
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    LOGINFO("Connected to the GemFire Distributed System");

    // Create a GemFire Cache with the "clientHACache.xml" Cache XML file.
    CachePtr cachePtr =
        cacheFactory->set("cache-xml-file", "XMLs/clientHACache.xml")
            ->addServer("localhost", 40404)
            ->addServer("localhost", 40405)
            ->setSubscriptionRedundancy(1)
            ->setSubscriptionEnabled(true)
            ->create();

    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML
    // file.
    RegionPtr regionPtr = cachePtr->getRegion("exampleRegion");

    LOGINFO("Obtained the Region from the Cache");

    // Register and Unregister Interest on Region for Some Keys.
    VectorOfCacheableKey keys;
    CacheableKeyPtr key1 = CacheableInt32::create(123);
    CacheableKeyPtr key2 = CacheableString::create("Key-123");
    keys.push_back(key1);
    keys.push_back(key2);
    regionPtr->registerKeys(keys);
    regionPtr->registerRegex("Key.*");

    LOGINFO("Called registerKeys() and registerKeysRegex()");

    regionPtr->put(key1, 1);
    regionPtr->put(key2, 2);

    LOGINFO("Called put() on Region");

    LOGINFO("Waiting for updates on keys");
    millisleep(10000);

    int count = 0;

    if (regionPtr->get(key1) == NULLPTR) {
      LOGINFO("Verified that key1 has been destroyed");
      count++;
    }

    if (regionPtr->get(key2) == NULLPTR) {
      LOGINFO("Verified that key2 has been destroyed");
      count++;
    }

    if (count == 2) {
      LOGINFO("Verified all updates");
    } else {
      LOGINFO("Could not verify all updates");
    }

    regionPtr->unregisterKeys(keys);
    regionPtr->unregisterRegex("Key.*");

    LOGINFO("Unregistered keys");

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

    return 0;
  }
  // An exception should not occur
  catch (const Exception& gemfireExcp) {
    LOGERROR("HACache GemFire Exception: %s", gemfireExcp.getMessage());

    return 1;
  }
}
