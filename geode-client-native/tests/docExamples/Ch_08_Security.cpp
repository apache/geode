/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_08_Security.hpp"

using namespace gemfire;
using namespace docExample;

//constructor
Security::Security()
{
}

//destructor
Security::~Security()
{
}

//start cacheserver
void Security::startServer()
{
  CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
}
//stop cacheserver
void Security::stopServer()
{
  CacheHelper::closeServer( 1 );
}

/**
* @brief Example 8.1 C++ Client Acquiring Credentials Programmatically.
* The following example shows a C++ client acquiring its credentials.
*/
void Security::example_8_1()
{
  PropertiesPtr secProp = Properties::create();
  secProp->insert("security-client-auth-factory",
    "createPKCSAuthInitInstance");
  secProp->insert("security-client-auth-library", "securityImpl");
  secProp->insert("security-keystorepath", "keystore/gemfire6.keystore");
  secProp->insert("security-alias", "gemfire6");
  secProp->insert("security-zkeystorepass", "gemfire");
  CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory(secProp);
}

int main(int argc, char* argv[])
{
  try {
    printf("\nSecurity EXAMPLES: Starting...");
    Security cp8;
    printf("\nSecurity EXAMPLES: Starting server...");
    cp8.startServer();
    printf("\nSecurity EXAMPLES: Running example 8.1...");
    cp8.example_8_1();
    printf("\nSecurity EXAMPLES: stopping server...");
    cp8.stopServer();
    printf("\nSecurity EXAMPLES: All Done.");
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
