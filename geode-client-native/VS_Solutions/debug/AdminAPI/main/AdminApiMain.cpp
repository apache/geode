/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
// this program is for Visual Studio Debugging of the Admin API
// this program acts as an Admin API 'member' of the Distributed System
// tests here SHOULD also be part of the 'testAdminAPI.cpp' unit tests.
//
#include "AdminApiMain.hpp"

static const int discovery_iterations = 1;

using namespace gemfire_admin;

AdminTest*  test;

int main( int argc, char**argv )
{
  test = new AdminTest();
  test->begin();

  printf("\npress 'q' to quit > ");

  while ( 1 ) {
    if(getchar() != 'q')
      break;
  }

  return 0;
}

void AdminTest::begin() {

  printf("AdminTest, begun");

  ////////////////////////////////////////////////////////
  //// USE ONLY ONE connect() CALL others for testing ////
  ////////////////////////////////////////////////////////
  //
  sys = AdministratedSystem::connect( "Bosco-dflt" );  // via gfcpp.properties or defaults
  //sys = AdministratedSystem::connect( "Rosco-mcast", "224.0.33.99", "10398" );
  //sys = AdministratedSystem::connect( "Cosco-mcast", "10.80.10.82" );

  for ( int i = 0; i < discovery_iterations; i++ )
    discover( i );

  printf("\n");

  sys->disconnect();
}

void AdminTest::discover( int iteration_num ) {

  VectorOfCacheApplication  apps;
  CacheApplicationPtr       app;
  VectorOfCacheRegion       regs;

  printf( "\n>>Discovery Iteration %d<<\n", iteration_num + 1 );

  sys->discover();
  sys->getCacheApplications( apps );

  for ( size_t i = 0; i < apps.size(); i++ ) {
    app = apps[i];

    if ( i > 0 ) 
      printf("\n");

    printf( "\nMemberId = %s", app->getId() );
    printf( "\nMemberType = %s", app->getMemberType() );
    printf( "\nHostAddress = %s", app->getHostAddress() );

    printf( "\n HostName = %s", app->getHostName() );
    printf( "\n Name = %s", app->getName() );
    printf( "\n OS = %s", app->getOperatingSystem() );
    printf( "\n License = %s", app->getLicense() );
    printf( "\n StatsEnabled = %d", app->getStatisticsEnabled() );
    printf( "\n StatsInterval = %d", app->getStatisticsInterval() );
    printf( "\n" );

    app->getRootRegions( regs );

    for ( size_t j = 0; j < regs.size(); j++ ) {
      regionize( 0, regs[j] );
    }
  }
  printf("\n");
}

void AdminTest::regionize( int level, CacheRegionPtr reg ) {

  VectorOfCacheRegion  subs;

  // indent properly
  //
  for ( int i = 0; i < level; i++ )
    printf("  ");

  printf( "   %s\n", reg->getFullPath() );

  reg->getSubRegions( subs );
  for ( size_t j = 0; j < subs.size(); j++ )
    regionize( level + 1, subs[j] );
}


