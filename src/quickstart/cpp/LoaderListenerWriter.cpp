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
 * The LoaderListenerWriter QuickStart Example.
 *
 * This example takes the following steps:
 *
 *  1. Create a Geode Cache.
 *  2. Get the example Region from the Cache.
 *  3. Set the SimpleCacheLoader, SimpleCacheListener and SimpleCacheWriter
 * plugins on the Region.
 *  4. Put 3 Entries into the Region.
 *  5. Update an Entry in the Region.
 *  6. Destroy an Entry in the Region.
 *  7. Invalidate an Entry in the Region.
 *  8. Get a new Entry from the Region.
 *  9. Get the destroyed Entry from the Region.
 * 10. Close the Cache.
 *
 */

// Include the Geode library.
#include <gfcpp/GeodeCppCache.hpp>

// Include the SimpleCacheLoader, SimpleCacheListener and SimpleCacheWriter
// plugins.
#include "plugins/SimpleCacheLoader.hpp"
#include "plugins/SimpleCacheListener.hpp"
#include "plugins/SimpleCacheWriter.hpp"

// Use the "geode" namespace.
using namespace apache::geode::client;

// The LoaderListenerWriter QuickStart example.
int main(int argc, char** argv) {
  try {
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    // Create a Geode Cache with the "clientLoaderListenerWriter.xml" Cache
    // XML file.
    CachePtr cachePtr =
        cacheFactory
            ->set("cache-xml-file", "XMLs/clientLoaderListenerWriter.xml")
            ->create();

    LOGINFO("Created the Geode Cache");

    // Get the example Region from the Cache which is declared in the Cache XML
    // file.
    RegionPtr regionPtr = cachePtr->getRegion("exampleRegion");

    LOGINFO("Obtained the Region from the Cache");

    // Plugin the SimpleCacheLoader, SimpleCacheListener and SimpleCacheWrite to
    // the Region.
    AttributesMutatorPtr attrMutatorPtr = regionPtr->getAttributesMutator();
    attrMutatorPtr->setCacheLoader(CacheLoaderPtr(new SimpleCacheLoader()));
    attrMutatorPtr->setCacheListener(
        CacheListenerPtr(new SimpleCacheListener()));
    attrMutatorPtr->setCacheWriter(CacheWriterPtr(new SimpleCacheWriter()));

    LOGINFO("Attached the simple plugins on the Region");

    // The following operations should cause the plugins to print the events.

    // Put 3 Entries into the Region.
    regionPtr->put("Key1", "Value1");
    regionPtr->put("Key2", "Value2");
    regionPtr->put("Key3", "Value3");

    // Update Key3.
    regionPtr->put("Key3", "Value3-updated");

    // Destroy Key3.
    regionPtr->destroy("Key3");

    // Invalidate Key2.
    regionPtr->invalidate("Key2");

    // Get a new Key.
    regionPtr->get("Key4");

    // Get a destroyed Key.
    regionPtr->get("Key3");

    // Close the Geode Cache.
    cachePtr->close();

    LOGINFO("Closed the Geode Cache");

    return 0;
  }
  // An exception should not occur
  catch (const Exception& geodeExcp) {
    LOGERROR("LoaderListenerWriter Geode Exception: %s",
             geodeExcp.getMessage());

    return 1;
  }
}
