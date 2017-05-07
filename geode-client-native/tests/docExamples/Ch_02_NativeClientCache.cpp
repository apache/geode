/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_02_NativeClientCache.hpp"

using namespace gemfire;
using namespace docExample;

const char * regionNames[] = { "exampleRegion0", "exampleRegion1" };

//constructor
NativeClientCache::NativeClientCache()
{
}

//destructor
NativeClientCache::~NativeClientCache()
{
}

//start cacheserver
void NativeClientCache::startServer()
{
  CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
}

//stop cacheserver
void NativeClientCache::stopServer()
{
  CacheHelper::closeServer( 1 );
}

//Initalize Region
void NativeClientCache::initRegion()
{
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regPtr0  = regionFactory->setLruEntriesLimit(20000) ->setCachingEnabled(true)
             ->create("exampleRegion0");
  regPtr1  = regionFactory->setLruEntriesLimit(20000) ->setCachingEnabled(true)
                 ->create("exampleRegion1");
}

void NativeClientCache::createValues()
{
  keyPtr1 = CacheableString::create("name1");
  keyPtr3 = CacheableString::create("name2");
}

/**
* @brief Example 2.3 Programmatically Registering Interest in a Specific Key.
*/
void NativeClientCache::example_2_3()
{
  keys0.push_back(keyPtr1);
  keys1.push_back(keyPtr3);
  regPtr0->registerKeys(keys0);
  regPtr1->registerKeys(keys1);
}

/**
* @brief Example 2.4 Programmatically Unregistering Interest in a Specific Key.
*/
void NativeClientCache::example_2_4()
{
  regPtr0->unregisterKeys(keys0);
  regPtr1->unregisterKeys(keys1);
}

/**
* @brief Example 2.5 Programmatically Registering Interest in All Keys.
*/
void NativeClientCache::example_2_5()
{
  regPtr0 = cachePtr->getRegion( regionNames[0] );
  regPtr1 = cachePtr->getRegion( regionNames[1] );
  regPtr0->registerAllKeys();
  regPtr1->registerAllKeys();
}

/**
* @brief Example 2.6 Programmatically Unregistering Interest in All Keys.
*/
void NativeClientCache::example_2_6()
{
  regPtr0 = cachePtr->getRegion( regionNames[0] );
  regPtr1 = cachePtr->getRegion( regionNames[1] );
  regPtr0->unregisterAllKeys();
  regPtr1->unregisterAllKeys();
}

/**
* @brief Example 2.7 Programmatically Registering Interest Using Regular Expressions.
*/
void NativeClientCache::example_2_7()
{
  regPtr1 = cachePtr->getRegion( regionNames[1] );
  regPtr1->registerRegex("Key-.*");
}

/**
* @brief Example 2.8 Programmatically Unregistering Interest Using Regular Expressions.
*/
void NativeClientCache::example_2_8()
{
  regPtr1 = cachePtr->getRegion( regionNames[1] );
  regPtr1->unregisterRegex("Key-.*");
}

/**
* @brief Example 2.9 Using serverKeys to Retrieve the Set of Keys From the Server.
*/
void NativeClientCache::example_2_9()
{
  VectorOfCacheableKey keysVec;
  regPtr0->serverKeys( keysVec );
  size_t vlen = keysVec.size();
  bool foundKey1 = false;
  bool foundKey2 = false;
  for( size_t i = 0; i < vlen; i++ ) {
    CacheableStringPtr strPtr = dynCast<CacheableStringPtr>
      ( keysVec.at( i ));
    std::string veckey = strPtr->asChar();
    if ( veckey == "skey1" ) {
      printf( "found skey1" );
      foundKey1 = true;
    }
    if ( veckey == "skey2" ) {
      printf( "found skey2" );
      foundKey2 = true;
    }
  }
}

/**
* @brief Example 2.12 Defining Callback After Region Disconnects From Cache Listener.
*/
class DisconnectCacheListener : public CacheListener
{
  void afterRegionDisconnected( const RegionPtr& region )
  {
    printf("After Region Disconnected event received");
  }
};

class TestCacheWriter : public CacheWriter
{
  void beforeRegionDisconnected( const RegionPtr& region )
  {
    printf("Before Region Disconnected event received");
  }
  bool beforeDestroy(const EntryEvent& event)
  {
    printf("\nTestCacheWriter: Got a beforeDestroy event.");
    return true;
  }
};

class TestCacheLoader : public CacheLoader
{
  int m_loads;
  CacheablePtr load(const RegionPtr& rp,
    const CacheableKeyPtr& key,
    const UserDataPtr& aCallbackArgument) {
      printf( "\nExampleCacheLoader.load : successful");
      return CacheableInt32::create(m_loads++);
  }
};

/**
* @brief Example 2.13 Adding a Cache Listener to a Region Dynamically.
*/
void NativeClientCache::setListener(RegionPtr& region)
{
  CacheListenerPtr regionListener(CacheListenerPtr(new DisconnectCacheListener()));
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  //Change cache listener for region.
  regionAttributesMutator->setCacheListener(regionListener);
}

/**
* @brief Example 2.14 Adding a Cache Listener Using a Dynamically Linked Library.
*/
void NativeClientCache::setListenerUsingFactory(RegionPtr& region)
{
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  //Change cache listener for region.
  regionAttributesMutator->setCacheListener("Library", "createTestCacheListener");
}

/**
* @brief Example 2.15 Removing a Cache Listener From a Region.
*/
void NativeClientCache::removeListener(RegionPtr& region)
{
  CacheListenerPtr nullListener = NULLPTR;
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  // Change cache listener for region to NULL.
  regionAttributesMutator->setCacheListener(nullListener);
}

/**
* @brief Example 2.16 Adding a Cache Writer to a Region Dynamically.
*/
void NativeClientCache::setWriter(RegionPtr& region)
{
  CacheWriterPtr regionWriter(CacheWriterPtr(new TestCacheWriter()));
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  // Change cache writer for region.
  regionAttributesMutator->setCacheWriter(regionWriter);
}

/**
* @brief Example 2.17 Adding a Cache Writer Using a Dynamically Linked Library.
*/
void NativeClientCache::setWriterUsingFactory(RegionPtr& region)
{
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  // Change cache writer for region.
  regionAttributesMutator->setCacheWriter("Library", "createTestCacheWriter");
}

/**
* @brief Example 2.18 Removing a Cache Writer From a Region.
*/
void NativeClientCache::removeWriter(RegionPtr& region)
{
  CacheWriterPtr nullWriter = NULLPTR;
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  // Change cache writer for region to NULL.
  regionAttributesMutator->setCacheWriter(nullWriter);
}

/**
* @brief Example 2.19 Adding a Cache Loader to a Region Dynamically.
*/
void NativeClientCache::setLoader(RegionPtr& region)
{
  CacheLoaderPtr regionLoader(CacheLoaderPtr(new TestCacheLoader()));
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  // Change cache loader for region.
  regionAttributesMutator->setCacheLoader(regionLoader);
}

/**
* @brief Example 2.20 Adding a Cache Loader Using a Dynamically Linked Library.
*/
void NativeClientCache::setLoaderUsingFactory(RegionPtr& region)
{
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  // Change cache loader for region.
  regionAttributesMutator->setCacheLoader("Library", "createTestCacheLoader");
}

/**
* @brief Example 2.21 Removing a Cache Loader From a Region.
*/
void NativeClientCache::removeLoader(RegionPtr& region)
{
  CacheLoaderPtr nullLoader = NULLPTR;
  AttributesMutatorPtr regionAttributesMutator = region->getAttributesMutator();
  // Change cache loader for region to NULL.
  regionAttributesMutator->setCacheLoader(nullLoader);
}

int main(int argc, char* argv[])
{
  try {
    printf("\nNativeClientCache EXAMPLES: Starting...");
    NativeClientCache cp2;
    printf("\nNativeClientCache EXAMPLES: Starting server...");
    cp2.startServer();
    CacheHelper::connectToDs(cp2.cacheFactoryPtr);
    printf("\nNativeClientCache EXAMPLES: Init Cache...");
    CacheHelper::initCache(cp2.cacheFactoryPtr, cp2.cachePtr);
    printf("\nNativeClientCache EXAMPLES: Init Region...");
    cp2.initRegion();
    printf("\nNativeClientCache EXAMPLES: Running example 2.3...");
    cp2.createValues();
    cp2.example_2_3();
    printf("\nNativeClientCache EXAMPLES: Running example 2.4...");
    cp2.example_2_4();
    CacheHelper::cleanUp(cp2.cachePtr);
    printf("\nNativeClientCache EXAMPLES: connecting to DS...");
    CacheHelper::connectToDs(cp2.cacheFactoryPtr);
    printf("\nNativeClientCache EXAMPLES: Init Cache...");
    CacheHelper::initCache(cp2.cacheFactoryPtr, cp2.cachePtr);
    printf("\nNativeClientCache EXAMPLES: Init Region...");
    cp2.initRegion();
    printf("\nNativeClientCache EXAMPLES: Running example 2.5...");
    cp2.example_2_5();
    printf("\nNativeClientCache EXAMPLES: Running example 2.6...");
    cp2.example_2_6();
    printf("\nNativeClientCache EXAMPLES: Running example 2.7...");
    cp2.example_2_7();
    printf("\nNativeClientCache EXAMPLES: Running example 2.8...");
    cp2.example_2_8();
    printf("\nNativeClientCache EXAMPLES: Running example 2.9...");
    cp2.example_2_9();

    //check for CacheLoader
    printf("\nNativeClientCache EXAMPLES: checking for CacheLoader...");
    cp2.setLoader(cp2.regPtr0);
    cp2.regPtr0->get(10);
    cp2.removeLoader(cp2.regPtr0);

    //check for CacheListener
    printf("\nNativeClientCache EXAMPLES: checking for CacheListener...");
    cp2.setListener(cp2.regPtr0);
    cp2.regPtr0->put("Key1", "Value1");
    cp2.regPtr0->put("Key1", "Value1-updated");
    cp2.removeListener(cp2.regPtr0);

    //check for CacheWriter
    printf("\nNativeClientCache EXAMPLES: checking for CacheWriter...");
    cp2.setWriter(cp2.regPtr0);
    cp2.regPtr0->destroy("Key1");
    cp2.removeWriter(cp2.regPtr0);

    CacheHelper::cleanUp(cp2.cachePtr);
    printf("\nNativeClientCache EXAMPLES: stopping server...");
    cp2.stopServer();
    printf("\nNativeClientCache EXAMPLES: All Done.");
  }catch (const Exception & excp)
  {
    printf("\nEXAMPLES: %s: %s", excp.getName(), excp.getMessage());
    exit(1);
  }
  catch(...)
  {
    printf("\nEXAMPLES: Unknown exception");
    exit(1);
  }
  return 0;
}
