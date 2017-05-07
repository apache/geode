/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "ExampleTestXml.hpp"

using namespace gemfire;
using namespace docExample;

//constructor
ExampleTestXml::ExampleTestXml()
{
}
//destructor
ExampleTestXml::~ExampleTestXml()
{
}

//start cacheserver
void ExampleTestXml::startServer()
{
  CacheHelper::initServer( 1, "server_xml.xml", "localhost:34756" );
}

//stop cacheserver
void ExampleTestXml::stopServer()
{
  CacheHelper::closeServer( 1 );
}

//start locator
void ExampleTestXml::startLocator()
{
  CacheHelper::initLocator(1);
}

//stop locator
void ExampleTestXml::stopLocator()
{
  CacheHelper::closeLocator(1);
}

/**
* @brief Testing Example 9.13 Creating an Index on a Cache Server Using a Server XML File.
*/
void ExampleTestXml::example_9_13()
{
	CacheHelper::initServer( 1, "DocXmlSnippetsServer.xml", "localhost:34756" );
}

/**
* @brief Testing Example 3.1 Declarative Cache Initialization with cache.xml.
*/
void ExampleTestXml::example_3_1()
{
	regionPtr = cachePtr->getRegion("root1");
	if (regionPtr != NULLPTR) {
		regionPtr->put(1,1);
	}
}
/**
* @brief Testing Example 2.1 Declarative Region Creation for a Native Client.
*/
void ExampleTestXml::example_2_1()
{
	regionPtr = cachePtr->getRegion("A");
	if (regionPtr != NULLPTR) {
		regionPtr->put(1,1);
	}
}

/**
* @brief Example 7.3 Overriding Region-Level Endpoints With Cache-Level Endpoints.
*/
void ExampleTestXml::example_7_3()
{
	regionPtr = cachePtr->getRegion("ThinClientRegion1");
	if (regionPtr != NULLPTR) {
		regionPtr->put(1,1);
	}
}

/**
* @brief Example 13.1 Declaring a Native Client Region Using cache.xml
*/
void ExampleTestXml::example_13_1()
{
	regionPtr = cachePtr->getRegion("NativeClientRegion");
	if (regionPtr != NULLPTR) {
		regionPtr->put(1,1);
	}
}

int main(int argc, char* argv[])
{
  try {
    printf("\nExampleTestXml EXAMPLES: Starting...");
    ExampleTestXml xml;
    printf("\nExampleTestXml EXAMPLES: Starting server to check example_9_13...");
    xml.example_9_13(); //Starting server with DocXmlSnippetsSever.xml file.
    printf("\nExampleTestXml EXAMPLES: stopping server...");
    xml.stopServer();
    printf("\nExampleTestXml EXAMPLES: Starting locator...");
    xml.startLocator();
    printf("\nExampleTestXml EXAMPLES: Starting server...");
    xml.startServer();
    printf("\nExampleTestXml EXAMPLES: connect to DS...");
    CacheHelper::connectToDs(xml.cacheFactoryPtr,"/DocXmlSnippets.xml");
    printf("\nExampleTestXml EXAMPLES: initialize cache...");
    CacheHelper::initCache(xml.cacheFactoryPtr, xml.cachePtr, "/DocXmlSnippets.xml");
    printf("\nExampleTestXml EXAMPLES: Example_13_1...");
    xml.example_13_1();
    CacheHelper::cleanUp(xml.cachePtr);
    printf("\nExampleTestXml EXAMPLES: stopping locator...");
    xml.stopLocator();
    CacheHelper::connectToDs(xml.cacheFactoryPtr, "/Example7_3.xml");
    CacheHelper::initCache(xml.cacheFactoryPtr, xml.cachePtr, "/Example7_3.xml");
    printf("\nExampleTestXml EXAMPLES: Example_7_3...");
    xml.example_7_3();
    CacheHelper::cleanUp(xml.cachePtr);
    printf("\nExampleTestXml EXAMPLES: connect to DS...");
    CacheHelper::connectToDs(xml.cacheFactoryPtr, "/Example_3_1.xml");
    printf("\nExampleTestXml EXAMPLES: initialize cache3.1...");
    CacheHelper::initCache(xml.cacheFactoryPtr, xml.cachePtr, "/Example_3_1.xml");
    printf("\nExampleTestXml EXAMPLES: Example_3_1...");
    xml.example_3_1();
    CacheHelper::cleanUp(xml.cachePtr);
    printf("\nExampleTestXml EXAMPLES: connect to DS...");
    CacheHelper::connectToDs(xml.cacheFactoryPtr, "/Example_2_1.xml");
	printf("\nExampleTestXml EXAMPLES: initialize cache2.1...");
	CacheHelper::initCache(xml.cacheFactoryPtr, xml.cachePtr, "/Example_2_1.xml");
	printf("\nExampleTestXml EXAMPLES: Example_2_1...");
	xml.example_2_1();
	CacheHelper::cleanUp(xml.cachePtr);
    printf("\nExampleTestXml EXAMPLES: stopping server...");
    xml.stopServer();
    printf("\nExampleTestXml EXAMPLES: All Done.");
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
