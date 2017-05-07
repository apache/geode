/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */


#include "fwklib/FwkLog.hpp"
#include "fwklib/FwkStrCvt.hpp"

using namespace gemfire;
using namespace gemfire::testframework;

// ----------------------------------------------------------------------------
    
/**
* DESCRIPTION:
*
*   Main function for C++ test client program.
*/

int main( int32_t argc, char * argv[] )
{

  /* Process Command Line args */
  if ( argc < 2 ) {
    FWKSEVERE( "Usage " << argv[0] << " <timestring> " );
    exit( 1 );
  }
  
  std::string timestr =  ACE_TEXT_ALWAYS_CHAR( argv[1] );
  int32_t secs = FwkStrCvt::toSeconds( timestr );
  if ( secs < 0 ) secs = 0;
  fprintf( stdout, "%d", secs );
  
  return 0;
}  /* main() */

