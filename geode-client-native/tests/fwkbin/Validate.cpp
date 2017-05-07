/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fwklib/FwkLog.hpp"
#include "fwklib/FwkObjects.hpp"


using namespace gemfire;
using namespace gemfire::testframework;


int main( int32_t argc, char * argv[] )
{
  /* Process Command Line args */
  FWKINFO( "In Validate" );
  if ( argc < 2 ) {
    FWKSEVERE( "Usage " << argv[0] << " <testFiles>" );
    return 1 ;
  }
  
  int32_t cnt = 0;
  int32_t bad = 0;
  for ( int32_t i = 1; i < argc; i++ ) {
    const char * testFile = ACE_TEXT_ALWAYS_CHAR( argv[i] ); // XML file 

    // load the test definition xml
    try {
      cnt++;
      TestDriver coll( testFile );
      FWKINFO( testFile << " OK." );
    } catch( FwkException ex ) {
      FWKSEVERE( "Unable to parse file " << testFile << ", caught exception: " << ex.getMessage() );
      bad++;
    }
  }
  FWKINFO( "Processed " << cnt << " files, " << bad << " failed to parse." );
  if ( bad > 0 ) {
    return -1; 
  }
  return 0;
}  /* main() */

