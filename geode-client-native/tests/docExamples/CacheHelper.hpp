/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_CACHEHELPER_HPP
#define TEST_CACHEHELPER_HPP

#include <gfcpp/GemfireCppCache.hpp>
#include <stdlib.h>
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>

#include <list>
#include "DistributedSystemImpl.hpp"
#include "Utils.hpp"
#include "PoolManager.hpp"
#ifndef ROOT_NAME
#define ROOT_NAME "Root"
#endif

#ifndef ROOT_SCOPE
#define ROOT_SCOPE LOCAL
#endif

#if defined(WIN32)
#define CACHESERVER_SCRIPT "cacheserver.bat"
#define GEMFIRE_SCRIPT "gemfire.bat"
#else
#define CACHESERVER_SCRIPT "cacheserver"
#define GEMFIRE_SCRIPT "gemfire"
#endif

using namespace gemfire;
namespace docExample {
  class CacheHelper
  {
  public:
    static CacheHelper* singleton;

    static CacheHelper& getHelper()
    {
      if ( singleton == NULL ) {
        singleton = new CacheHelper();
      }
      return *singleton;
    }

    virtual ~CacheHelper( )
    {
    }

    /*
    * GFJAVA is the environment variable. user has to set GFJAVA variable as a product build directory
    * path for java cache server or set as endpoints list for the remote server
    */

    static const char * getTcrEndpoints( bool & isLocalServer, int numberOfServers = 1 )
    {
      static char * gfjavaenv = ACE_OS::getenv( "GFJAVA" );
      std::string gfendpoints;
      static bool gflocalserver = false;

      if ( gfendpoints.empty() ) {
        if ( ( ACE_OS::strchr( gfjavaenv,'\\') != NULL ) || ( ACE_OS::strchr( gfjavaenv,'/') != NULL ) ) {
          gflocalserver = true;
          /* Support for multiple servers Max = 10*/
          switch( numberOfServers ) {
  case 1:
    gfendpoints = "localhost:24680";
    break;
  case 2:
    gfendpoints = "localhost:24680,localhost:24681";
    break;
  case 3:
    gfendpoints = "localhost:24680,localhost:24681,localhost:24682";
    break;
  default:
    gfendpoints = "localhost:24680";
    char temp[8];
    for(int i =1; i <= numberOfServers - 1; i++) {
      gfendpoints += ",localhost:2468";
      gfendpoints += ACE_OS::itoa(i,temp,10);
    }
    break;
          }
        }
        else {
          gfendpoints = gfjavaenv;
        }
      }
      isLocalServer = gflocalserver;
      return ( new std::string( gfendpoints.c_str( ) ) )->c_str( );
    }
    static const char * getLocatorHostPort( bool & isLocator, int numberOfLocators = 0 )
    {
      static char * gfjavaenv = ACE_OS::getenv( "GFJAVA" );
      static std::string gflchostport;
      static bool gflocator = false;

      if ( gflchostport.empty() ) {
        if ( ( ACE_OS::strchr( gfjavaenv,'\\') != NULL ) || ( ACE_OS::strchr( gfjavaenv,'/') != NULL ) ) {
          gflocator = true;
          switch( numberOfLocators ) {
  case 1:
    gflchostport = "localhost:34756";
    break;
  case 2:
    gflchostport = "localhost:34756,localhost:34757";
    break;
  default:
    gflchostport = "localhost:34756,localhost:34757,localhost:34758";
    break;
          }
        }
        else {
          gflchostport = "";
        }
      }
      isLocator = gflocator;
      return gflchostport.c_str();
    }

    static void initServer( int instance, const char * xml = NULL ,const char *locHostport = NULL , const char *authParam = NULL) {
      if (authParam != NULL) {
        printf("Inside initServer with authParam = %s\n", authParam);
      }
      else {
        printf("Inside initServer with authParam as NULL\n");
        authParam = "";
      }
      static const char* gfjavaenv = ACE_OS::getenv("GFJAVA");
      static const char* gfLogLevel = ACE_OS::getenv("GFE_printfLEVEL");
      static const char* gfSecLogLevel = ACE_OS::getenv("GFE_SECprintfLEVEL");
      static const char* path = ACE_OS::getenv("TESTSRC");
      static const char* mcastPort = ACE_OS::getenv("MCAST_PORT");
      static const char* mcastAddr = ACE_OS::getenv("MCAST_ADDR");

      char cmd[1024];
      char currWDPath[1024];
      std::string currDir = ACE_OS::getcwd( currWDPath, 1024 );
      if(  gfjavaenv == NULL || path == NULL || mcastPort == NULL || mcastAddr == NULL || currDir.empty())
      {
        printf("GFJAVA or TESTSRC or MCAST_PORT or MCAST_ADDR or current directory is not set\n");
        exit(1);
      }
      if (gfLogLevel == NULL || gfLogLevel[0] == '\0') {
        gfLogLevel = "config";
      }
      if (gfSecLogLevel == NULL || gfSecLogLevel[0] == '\0') {
        gfSecLogLevel = "config";
      }

      if ( ( ACE_OS::strchr( gfjavaenv,'\\') == NULL ) && ( ACE_OS::strchr( gfjavaenv,'/') == NULL ) )
        return;
      std::string xmlFile = path;
      std::string sname = "GFECS";
      currDir += "/";

      switch( instance ) {
  case 1:
    xmlFile += "/cacheserver.xml";
    sname += "1";
    break;
  case 2:
    xmlFile += "/cacheserver2.xml";
    sname += "2";
    break;
  case 3:
    xmlFile += "/cacheserver3.xml";
    sname += "3";
    break;
  default:  /* Support for any number of servers Max 10*/
    char temp[8];
    sname += ACE_OS::itoa(instance,temp,10);
    break;
      }

      currDir += sname;

      if ( xml != NULL ) {
        xmlFile = path;
        xmlFile += "/";
        xmlFile += xml;
      }

      ACE_OS::mkdir( sname.c_str() );

      sprintf( cmd, "/bin/cp %s/../gemfire.properties %s/",currDir.c_str(),
        currDir.c_str()  );
      printf( "%s\n", cmd );
      ACE_OS::system( cmd );

      sprintf(cmd, "%s/bin/%s stop -dir=%s", gfjavaenv, CACHESERVER_SCRIPT,
        currDir.c_str());

      printf( "%s\n", cmd );
      ACE_OS::system( cmd );
      if (locHostport != NULL) { // check number of locator host port.
        sprintf(cmd, "%s/bin/%s start -J-Xmx1024m -J-Xms128m locators=%s "
          "cache-xml-file=%s -dir=%s mcast-port=0 log-level=%s "
          "security-log-level=%s %s", gfjavaenv, CACHESERVER_SCRIPT,
          locHostport, xmlFile.c_str(), currDir.c_str(), gfLogLevel,
          gfSecLogLevel, authParam);
      }
      else {
        sprintf(cmd, "%s/bin/%s start -J-Xmx1024m -J-Xms128m cache-xml-file=%s "
          "-dir=%s mcast-address=%s mcast-port=%s log-level=%s "
          "security-log-level=%s %s", gfjavaenv, CACHESERVER_SCRIPT,
          xmlFile.c_str(), currDir.c_str(), mcastAddr, mcastPort, gfLogLevel,
          gfSecLogLevel, authParam);
      }

      printf( "%s\n", cmd );
      ACE_OS::system( cmd );

      printf("added server instance %d\n", instance);
    }
    static void  initGFMOnAgent(int instance=1, const char* rmiPort = "12345")
    {
      static char * gfjavaenv = ACE_OS::getenv( "GFJAVA" );
      //static char * path = ACE_OS::getenv( "TESTSRC" );
      static char* mcastPort = ACE_OS::getenv( "MCAST_PORT" );
      static char * mcastAddr = ACE_OS::getenv( "MCAST_ADDR" );
      char cmd[1024];
      char currWDPath[1024];
      std::string currDir = ACE_OS::getcwd( currWDPath, 1024 );
      char inst[3];
      printf("mcast-port=%s\n", mcastPort);
      sprintf(inst,"%d",instance);
      currDir+="/";
      std::string sname = "GFECS";
      sname.append(inst);
      ACE_OS::mkdir( sname.c_str() );
      currDir+=sname;
      sprintf( cmd, "%s/bin/%s stop -dir=%s", gfjavaenv, "agent", currDir.c_str() );
      ACE_OS::system( cmd );
      bool isLocator = true;
      const char* locHostPort = getLocatorHostPort( isLocator );
      sprintf( cmd, "%s/bin/%s start rmi-port=%s mcast-address=%s mcast-port=0 locators=%s -dir=%s", gfjavaenv,"agent", rmiPort ,mcastAddr, locHostPort/*mcastPort*/, currDir.c_str() );
      printf("%s\n", cmd);
      ACE_OS::system( cmd );
    }
    static void closeGFMOnAgent(int instance)
    {
      static char * gfjavaenv = ACE_OS::getenv( "GFJAVA" );
      //static char * path = ACE_OS::getenv( "TESTSRC" );

      char cmd[1024];
      char currWDPath[1024];
      std::string currDir = ACE_OS::getcwd( currWDPath, 1024 );
      char inst[3];
      sprintf(inst,"%d",instance);
      currDir+="/";
      std::string sname = "GFECS";
      sname.append(inst);
      ACE_OS::mkdir( sname.c_str() );
      currDir+=sname;

      ACE_OS::mkdir( sname.c_str() );
      sprintf( cmd, "%s/bin/%s stop -dir=%s", gfjavaenv, "agent", currDir.c_str() );
      printf( "%s\n", cmd );
      ACE_OS::system( cmd );
    }
    static void closeServer( int instance )
    {
      static char * gfjavaenv = ACE_OS::getenv( "GFJAVA" );

      char cmd[1024];
      char currWDPath[1024];

      std::string currDir = ACE_OS::getcwd( currWDPath, 1024 );
      if( gfjavaenv == NULL){
        printf("Environment variable GFJAVA for java build directory is not set.\n");
        exit(1);
      }
      if(currDir.empty()){
        printf("Current working directory could not be determined." );
        exit(1);
      }
      if ( ( ACE_OS::strchr( gfjavaenv,'\\') == NULL ) && ( ACE_OS::strchr( gfjavaenv,'/') == NULL ) )
        return;

      currDir += "/GFECS";
      switch( instance ) {
  case 1:
    currDir += "1";
    break;
  case 2:
    currDir += "2";
    break;
  case 3:
    currDir += "3";
    break;
  default: /* Support for any number of servers Max 10*/
    char temp[8];
    currDir += ACE_OS::itoa(instance,temp,10);
    break;
      }

      sprintf(cmd, "%s/bin/%s stop -dir=%s", gfjavaenv, CACHESERVER_SCRIPT,
        currDir.c_str());

      printf( "%s\n", cmd );
      ACE_OS::system( cmd );
    }
    // closing locator
    static void closeLocator( int instance )
    {
      static char * gfjavaenv = ACE_OS::getenv( "GFJAVA" );

      char cmd[1024];
      char currWDPath[1024];
      int portnum = 0;
      std::string currDir = ACE_OS::getcwd( currWDPath, 1024 );

      if( gfjavaenv == NULL){
        printf("Environment variable GFJAVA for java build directory is not set.\n");
        exit(1);
      }
      if(currDir.empty()){
        printf("Current working directory could not be determined." );
        exit(1);
      }
      if ( ( ACE_OS::strchr( gfjavaenv,'\\') == NULL ) && ( ACE_OS::strchr( gfjavaenv,'/') == NULL ) )
        return;

      currDir += "/GFELOC";
      switch( instance ) {
  case 1:
    portnum = 34756;
    currDir += "1";
    break;
  case 2:
    portnum = 34757;
    currDir += "2";
    break;
  case 3:
    portnum = 34758;
    currDir += "3";
    break;
  default: /* Support for any number of Locator Max 10*/
    char temp[8];
    currDir += ACE_OS::itoa(instance,temp,10);
    break;
      }
      sprintf( cmd, "%s/bin/%s stop-locator -port=%d -dir=%s", gfjavaenv, GEMFIRE_SCRIPT, portnum,currDir.c_str() );

      printf( "%s\n", cmd );
      ACE_OS::system( cmd );

    }

    // starting locator
    static void initLocator( int instance) {
      static char * gfjavaenv = ACE_OS::getenv( "GFJAVA" );

      char cmd[1024];
      char currWDPath[1024];
      std::string currDir = ACE_OS::getcwd( currWDPath, 1024 );

      if( gfjavaenv == NULL){
        printf("Environment variable GFJAVA for java build directory is not set.\n");
        exit(1);
      }
      if(currDir.empty()){
        printf("Current working directory could not be determined." );
        exit(1);
      }

      if ( ( ACE_OS::strchr( gfjavaenv,'\\') == NULL ) && ( ACE_OS::strchr( gfjavaenv,'/') == NULL ) )
        return;
      std::string locDirname = "GFELOC";
      int portnum = 0;
      currDir += "/";

      switch( instance ) {
  case 1:
    portnum = 34756;
    locDirname += "1";
    break;
  case 2:
    portnum = 34757;
    locDirname += "2";
    break;
  default:
    portnum = 34758;
    locDirname += "3";
    break;
      }

      currDir += locDirname;

      ACE_OS::mkdir( locDirname.c_str() );
      std::string gemfireFile = currDir;
      gemfireFile += "/gemfire.properties";
      FILE* urandom = ACE_OS::fopen(gemfireFile.c_str(), "w+");
      std::string msg = "locators=localhost[34756],localhost[34757],localhost[34758]";
      ACE_OS::fwrite( msg.c_str(), msg.size(), 1, urandom );
      ACE_OS::fflush( urandom );
      ACE_OS::fclose( urandom);
      printf( "%s\n", gemfireFile.c_str());
      sprintf(cmd, "%s/bin/%s stop-locator -port=%d -dir=%s", gfjavaenv,
        GEMFIRE_SCRIPT, portnum, currDir.c_str());

      printf( "%s\n", cmd );
      ACE_OS::system( cmd );

      sprintf(cmd, "%s/bin/%s start-locator -port=%d -dir=%s",
        gfjavaenv, GEMFIRE_SCRIPT, portnum, currDir.c_str());

      printf( "%s\n", cmd );
      ACE_OS::system( cmd );
    }

    //connect to DS
    static void connectToDs(CacheFactoryPtr& cacheFactoryPtr,const char* xmlfile = NULL)
    {
      PropertiesPtr prptr = Properties::create();
      if(xmlfile != NULL) {
        static const char* path = ACE_OS::getenv("TESTSRC");
        std::string xmlFile = path;
        xmlFile += xmlfile;
        prptr->insert("cache-xml-file", xmlFile.c_str());
      }
      cacheFactoryPtr = CacheFactory::createCacheFactory(prptr);
    }

    static void initCache(CacheFactoryPtr& cacheFactoryPtr, CachePtr& cachePtr,const char* xmlfile = NULL)
    {
      if(cacheFactoryPtr != NULLPTR) {
        if(xmlfile != NULL) {
          cachePtr = cacheFactoryPtr->create();
        }
        else {
          cachePtr = cacheFactoryPtr->setSubscriptionEnabled(true)->addServer("localhost", 24680)->addServer("localhost", 24681)->create();
        }
      }
    }

    //init cache
    static void initCache(CacheFactoryPtr& cacheFactoryPtr, CachePtr& cachePtr, bool isPool)
    {
      cachePtr = cacheFactoryPtr->create();
    }

    static void cleanUp(CachePtr cachePtr)
    {
      cachePtr->close();
      cachePtr= NULLPTR;
    }

  };

  CacheHelper* CacheHelper::singleton = NULL;

} // namespace docExample
#endif // TEST_CACHEHELPER_HPP



