/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientTasks_C2S2.hpp
 *
 *  Created on: Nov 3, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTTASKS_C2S2_HPP_
#define THINCLIENTTASKS_C2S2_HPP_

// define our own names for the 4 test processes
#define PROCESS1 s1p1
#define PROCESS2 s1p2
#define PROCESS3 s2p1
#define PROCESS4 s2p2

DUNIT_TASK_DEFINITION( SERVER1 , StartLocator )
{
  CacheHelper::initLocator( 1 );
  LOG("Locator started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( SERVER1 , CloseLocator )
{
  CacheHelper::closeLocator( 1 );
  LOG("Locator stopped");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS1 , Process1_SetPoolFlag )
{
  g_poolConfig = true;
  LOG("PoolConfig = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS2 , Process2_SetPoolFlag )
{
  g_poolConfig = true;
  LOG("PoolConfig = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS3 , Process3_SetPoolFlag )
{
  g_poolConfig = true;
  LOG("PoolConfig = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS4 , Process4_SetPoolFlag )
{
  g_poolConfig = true;
  LOG("PoolConfig = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS1 , Process1_SetPoolFlagFalse )
{
  g_poolConfig = false;
  LOG("PoolConfig = false");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS2 , Process2_SetPoolFlagFalse )
{
  g_poolConfig = false;
  LOG("PoolConfig = false");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS3 , Process3_SetPoolFlagFalse )
{
  g_poolConfig = false;
  LOG("PoolConfig = false");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS4 , Process4_SetPoolFlagFalse )
{
  g_poolConfig = false;
  LOG("PoolConfig = false");
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION( PROCESS1 , Process1_SetPoolLocatorFlag )
{
  g_poolLocators = true;
  LOG("PoolLocator = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS2 , Process2_SetPoolLocatorFlag )
{
  g_poolLocators = true;
  LOG("PoolLocator = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS3 , Process3_SetPoolLocatorFlag )
{
  g_poolLocators = true;
  LOG("PoolLocator = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS4 , Process4_SetPoolLocatorFlag )
{
  g_poolLocators = true;
  LOG("PoolLocator = true");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS1 , Process1_SetPoolLocatorFlagFalse )
{
  g_poolLocators = false;
  LOG("PoolLocator = false");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS2 , Process2_SetPoolLocatorFlagFalse )
{
  g_poolLocators = false;
  LOG("PoolLocator = false");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS3 , Process3_SetPoolLocatorFlagFalse )
{
  g_poolLocators = false;
  LOG("PoolLocator = false");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION( PROCESS4 , Process4_SetPoolLocatorFlagFalse )
{
  g_poolLocators = false;
  LOG("PoolLocator = false");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServers)
{
  if ( isLocalServer ) {
    CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml");
    CacheHelper::initServer( 2 , "cacheserver_notify_subscription2.xml");
  }
  LOG("SERVERs started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, startServer)
{
  if ( isLocalServer ) {
    CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml");    
  }
  LOG("SERVER started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServersWithLocator )
{
  if ( isLocalServer ) {
    CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml", locatorsG );
    CacheHelper::initServer( 2, "cacheserver_notify_subscription2.xml", locatorsG );
  }
  LOG("SERVERs started with locator");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, startServerWithLocator )
{
  if ( isLocalServer ) {
    CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml", locatorsG );    
  }
  LOG("SERVER started with locator");
}
END_TASK_DEFINITION

void initLocatorSettings( bool poolConfig = true, bool poolLocators = true )
{
  g_poolConfig = poolConfig;
  g_poolLocators = poolLocators;

  // AB: Global pool flags.
  // [todo]The following code will set global flags in the clients. This is a complicated way of setting globals, and should be further simplified.
  if ( poolConfig ) {
    CALL_TASK( Process1_SetPoolFlag );
    CALL_TASK( Process2_SetPoolFlag );
    CALL_TASK( Process3_SetPoolFlag );
    CALL_TASK( Process4_SetPoolFlag );

    if ( poolLocators ) {
      CALL_TASK( Process1_SetPoolLocatorFlag );
      CALL_TASK( Process2_SetPoolLocatorFlag );
      CALL_TASK( Process3_SetPoolLocatorFlag );
      CALL_TASK( Process4_SetPoolLocatorFlag );
    }else {
      CALL_TASK( Process1_SetPoolLocatorFlagFalse );
      CALL_TASK( Process2_SetPoolLocatorFlagFalse );
      CALL_TASK( Process3_SetPoolLocatorFlagFalse );
      CALL_TASK( Process4_SetPoolLocatorFlagFalse );
    }
  }else {
    CALL_TASK( Process1_SetPoolFlagFalse );
    CALL_TASK( Process2_SetPoolFlagFalse );
    CALL_TASK( Process3_SetPoolFlagFalse );
    CALL_TASK( Process4_SetPoolFlagFalse );
    
    CALL_TASK( Process1_SetPoolLocatorFlagFalse );
    CALL_TASK( Process2_SetPoolLocatorFlagFalse );
    CALL_TASK( Process3_SetPoolLocatorFlagFalse );
    CALL_TASK( Process4_SetPoolLocatorFlagFalse );
  }
}

void startServers( )
{
  if ( g_poolConfig && g_poolLocators ) {
    CALL_TASK( StartServersWithLocator );
  }
  else {
    CALL_TASK( StartServers );
  }
}

void startServer( )
{
  if ( g_poolConfig && g_poolLocators ) {
    CALL_TASK( startServerWithLocator );
  }
  else {
    CALL_TASK( startServer );
  }
}

void closeLocator( )
{
  if ( g_poolConfig && g_poolLocators ) {
    CALL_TASK( CloseLocator );
  }
}

#endif /* THINCLIENTTASKS_C2S2_HPP_ */
