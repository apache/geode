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
 * The Durable Client QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a Geode Cache with durable client properties Programmatically.
 * 2. Create the example Region Programmatically.
 * 3. Set Cacelistener with "afterRegionLive implementation to region.
 * 4. Register Interest to region with durable option
 * 5. call to readyForEvent().
 * 6. Close the Cache with keepalive options as true.
 *
 */

// Include the Geode library.
#include <gfcpp/GeodeCppCache.hpp>

// Include cachelistener
#include "plugins/DurableCacheListener.hpp"

using namespace apache::geode::client;

void RunDurableClient() {
  // Create durable client's properties using api.
  PropertiesPtr pp = Properties::create();
  pp->insert("durable-client-id", "DurableClientId");
  pp->insert("durable-timeout", 300);

  // Create a Geode Cache Programmatically.
  CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(pp);
  CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)->create();

  LOGINFO("Created the Geode Cache Programmatically");

  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);

  // Create the Region Programmatically.
  RegionPtr regionPtr = regionFactory->create("exampleRegion");

  LOGINFO("Created the Region Programmatically");

  // Plugin the CacheListener with afterRegionLive. "afterRegionLive()"  will be
  // called
  // after all the queued events are recieved by client
  AttributesMutatorPtr attrMutatorPtr = regionPtr->getAttributesMutator();
  attrMutatorPtr->setCacheListener(
      CacheListenerPtr(new DurableCacheListener()));

  LOGINFO("DurableCacheListener set to region.");

  // For durable Clients, Register Intrest can be durable or non durable (
  // default ),
  // Unregister Interest APIs remain same.

  VectorOfCacheableKey keys;
  keys.push_back(CacheableString::create("Key-1"));
  regionPtr->registerKeys(keys, true);

  LOGINFO("Called Register Interest for Key-1 with isDurable as true");

  // Send ready for Event message to Server( only for Durable Clients ).
  // Server will send queued events to client after recieving this.
  cachePtr->readyForEvents();

  LOGINFO("Sent ReadyForEvents message to server");

  // wait for some time to recieve events
  apache::geode::client::millisleep(1000);

  // Close the Geode Cache with keepalive = true.  Server will queue events
  // for
  // durable registered keys and will deliver all events when client will
  // reconnect
  // within timeout period and send "readyForEvents()"
  cachePtr->close(true);

  LOGINFO("Closed the Geode Cache with keepalive as true");
}
void RunFeeder() {
  CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
  LOGINFO("Feeder connected to the Geode Distributed System");

  CachePtr cachePtr = cacheFactory->create();

  LOGINFO("Created the Geode Cache");

  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(PROXY);

  LOGINFO("Created the RegionFactory");

  // Create the Region Programmatically.
  RegionPtr regionPtr = regionFactory->create("exampleRegion");

  LOGINFO("Created the Region Programmatically.");

  // create two keys in region
  const char* keys[] = {"Key-1", "Key-2"};
  const char* vals[] = {"Value-1", "Value-2"};

  for (int i = 0; i < 2; i++) {
    CacheableKeyPtr keyPtr = createKey(keys[i]);
    CacheableStringPtr valPtr = CacheableString::create(vals[i]);

    regionPtr->create(keyPtr, valPtr);
  }
  LOGINFO(
      "Created Key-1 and Key-2 in region. Durable interest was registered only "
      "for Key-1.");

  // Close the Geode Cache
  cachePtr->close();

  LOGINFO("Closed the Geode Cache");
}

// The DurableClient QuickStart example.
int main(int argc, char** argv) {
  try {
    // First Run of Durable Client
    RunDurableClient();

    // Intermediate Feeder, feeding events
    RunFeeder();

    // Reconnect Durable Client
    RunDurableClient();

    return 0;
  }
  // An exception should not occur
  catch (const Exception& geodeExcp) {
    LOGERROR("DurableClient Geode Exception: %s", geodeExcp.getMessage());

    return 1;
  }
}
