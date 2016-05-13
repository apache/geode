/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_06_SettingProperties.hpp"

using namespace gemfire;
using namespace docExample;

//constructor
SettingProperties::SettingProperties()
{
}

//destructor
SettingProperties::~SettingProperties()
{
}

//start cacheserver
void SettingProperties::startServer()
{
  CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
}

//stop cacheserver
void SettingProperties::stopServer()
{
  CacheHelper::closeServer( 1 );
}

/**
* @brief Example 6.1 Defining System Properties Programmatically.
* Following example creates a Properties object and defines all needed properties
* individually in that object.
*/
void SettingProperties::example_6_1()
{
  PropertiesPtr systemProps = Properties::create();
  systemProps->insert( "statistic-archive-file", "stats.gfs" );
  systemProps->insert( "cache-xml-file", "./myapp-cache.xml" );
  systemProps->insert( "stacktrace-enabled", "true" );
  CacheFactoryPtr systemPtr = CacheFactory::createCacheFactory(systemProps);
}

int main(int argc, char* argv[])
{
  try {
    printf("\nSettingProperties EXAMPLES: Starting...");
    SettingProperties cp6;
    printf("\nSettingProperties EXAMPLES: Starting server...");
    cp6.startServer();
    printf("\nSettingProperties EXAMPLES: Running example 6.1...");
    cp6.example_6_1();
    printf("\nSettingProperties EXAMPLES: stopping server...");
    cp6.stopServer();
    printf("\nSettingProperties EXAMPLES: All Done.");
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
