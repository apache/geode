/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "FileProcessor.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/FwkObjects.hpp"


using namespace gemfire;
using namespace gemfire::testframework;


/**  
* DESCRIPTION:
* 
*   Main function for C++ file processor program.
*/

int main( int32_t argc, char * argv[] )
{
  /* Process Command Line args */
  if ( argc < 2 ) {
    FWKSEVERE( "Usage " << argv[0] << " <testFile>" );
    return 1 ;
  }
  
  const char * testFile = ACE_TEXT_ALWAYS_CHAR( argv[1] ); // XML file containing all required test info

  // load the test definition xml
  try {
    TestDriver coll( testFile );
    coll.writeFiles();
  } catch( FwkException ex ) {
    FWKSEVERE( "Unable to write files, caught exception: " << ex.getMessage() );
    return -1;
  }
  
  return 0;
}  /* main() */

