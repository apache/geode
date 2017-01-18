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
 * 1. Create CacheFactory using the user specified properties or from the
 * gfcpp.properties file by default.
 * 2. Create a GemFire Cache.
 * 3. Get the example Region from the Cache.
 * 4. Call registerAllKeys() and unregisterAllKeys() on the Region.
 * 5. Call registerKeys() and unregisterKeys() on the Region.
 * 6. Call registerRegex() and unregisterRegex() on the Region.
 * 7. Close the Cache.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace apache::geode::client;

// The RegisterInterest QuickStart example.
int main(int argc, char** argv) {
  try {
    // Create CacheFactory using the user specified properties or from the
    // gfcpp.properties file by default.
    PropertiesPtr prp = Properties::create();
    prp->insert("cache-xml-file", "XMLs/clientRegisterInterest.xml");

    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(prp);

    LOGINFO("Created CacheFactory");

    // Create a GemFire Cache with the "clientRegisterInterest.xml" Cache XML
    // file.
    CachePtr cachePtr = cacheFactory->create();

    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML
    // file.
    RegionPtr regionPtr = cachePtr->getRegion("exampleRegion");

    LOGINFO("Obtained the Region from the Cache");

    // Register and Unregister Interest on Region for All Keys.
    regionPtr->registerAllKeys();
    regionPtr->unregisterAllKeys();

    LOGINFO("Called registerAllKeys() and unregisterAllKeys()");

    // Register and Unregister Interest on Region for Some Keys.
    VectorOfCacheableKey keys;
    keys.push_back(CacheableInt32::create(123));
    keys.push_back(CacheableString::create("Key-123"));
    regionPtr->registerKeys(keys);
    regionPtr->unregisterKeys(keys);

    LOGINFO("Called registerKeys() and unregisterKeys()");

    // Register and Unregister Interest on Region for Keys matching a Regular
    // Expression.
    regionPtr->registerRegex("Keys-*");
    regionPtr->unregisterRegex("Keys-*");

    LOGINFO("Called registerRegex() and unregisterRegex()");

    // Register Interest on Region for All Keys with getInitialValues to
    // populate the cache with values of all keys from the server.
    regionPtr->registerAllKeys(
        false, NULLPTR, true);  // Where the 3rd argument is getInitialValues.
    // Unregister Interest on Region for All Keys.
    regionPtr->unregisterAllKeys();

    LOGINFO(
        "Called registerAllKeys() and unregisterAllKeys() with "
        "getInitialValues argument");

    // Register Interest on Region for Some Keys with getInitialValues.
    keys.push_back(CacheableInt32::create(123));
    keys.push_back(CacheableString::create("Key-123"));
    regionPtr->registerKeys(
        keys, false, true);  // Where the 3rd argument is getInitialValues.

    LOGINFO(
        "Called registerKeys() and unregisterKeys() with getInitialValues "
        "argument");
    // Unregister Interest on Region for Some Keys.
    regionPtr->unregisterKeys(keys);

    // Register and Unregister Interest on Region for Keys matching a Regular
    // Expression with getInitialValues.
    regionPtr->registerRegex("Keys-*", false, NULLPTR, true);
    regionPtr->unregisterRegex("Keys-*");

    LOGINFO(
        "Called registerRegex() and unregisterRegex() with getInitialValues "
        "argument");

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

    return 0;
  }
  // An exception should not occur
  catch (const Exception& gemfireExcp) {
    LOGERROR("RegisterInterest GemFire Exception: %s",
             gemfireExcp.getMessage());

    return 1;
  }
}
