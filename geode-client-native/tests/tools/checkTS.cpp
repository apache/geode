/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

// To run:
//    On hostA: checkTS <port>
//    On hostB: checkTS <port>
//    On hostC: checkTS <port> -s 
// hostC will send time sync mesagees to hostA and hostB, who will print messages
// indicating the delta between themselves and hostC

#include "fwklib/FwkLog.hpp"
#include "fwklib/TimeSync.hpp"
#include "fwklib/PerfFwk.hpp"

using namespace gemfire;
using namespace gemfire::testframework;

int32_t usage() {
  FWKINFO( "First argument is required and must be the port number." );
  FWKINFO( "The second argument '-s' is only required by the process that will send updates." );
  return -1;
}

int main( int argc, char * argv[] ) {
  if ( argc < 2 ) {
    return usage();
  }
  
  bool client = true;
  int32_t port = atoi( argv[1] );
  if ( argc > 2 ) {
    client = false;
  }
  
  int32_t volatile delta = 0;
  int32_t * pDelta = NULL;
  
  if ( client ) {
    pDelta = ( int32_t * )&delta;
  }
  if ( client ) {
    FWKINFO( "Listening for server." );
  }
  else {
    FWKINFO( "Starting server." );
  }
  
  TimeSync ts( port, pDelta, client );
  while ( 1 ) {
    perf::sleepSeconds( 30 );
  }
  
  return 0;
}

