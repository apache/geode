/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */


#include "fwklib/FwkLog.hpp"
#include "fwklib/FwkBBClient.hpp"
#include "fwklib/FwkStrCvt.hpp"

#include <string.h>
#include <string>

using namespace gemfire;
using namespace gemfire::testframework;

#ifdef WIN32

#define strcasecmp stricmp

#endif // WIN32

// ----------------------------------------------------------------------------
    
/**
* DESCRIPTION:
*
*   Main function for C++ test client program.
*/

int main( int32_t argc, char * argv[] )
{

  /* Process Command Line args */
  if ( argc < 4 ) {
    FWKSEVERE( "Usage " << argv[0] << " <get,set,getInt,setInt,inc,dec> <BB name> <name> [value] " );
    exit(1);
  }
  
  char * op =  ACE_TEXT_ALWAYS_CHAR( argv[1] );
  std::string BBname = ACE_TEXT_ALWAYS_CHAR( argv[2] );
  std::string name = ACE_TEXT_ALWAYS_CHAR( argv[3] );
  std::string value = "";
  if ( argc == 5 ) {
    value = ACE_TEXT_ALWAYS_CHAR( argv[4] );
  }
  
  int retVal = 0;

  // Get GF_BBADDR value from the environment
  char * bbAddr = getenv( "GF_BBADDR" );
  if ( bbAddr == NULL ) {
    FWKSEVERE( "GF_BBADDR is not set in the environment, exiting." );
    exit( -1 );
  }
  
  FwkBBClient * bbc = new FwkBBClient( bbAddr );

  std::string result;

  if ( strcasecmp( op, "get" ) == 0 ) {
    result = bbc->getString( BBname, name );
  }
  else if ( strcasecmp( op, "set" ) == 0 ) {
    bbc->set( BBname, name, value );
  }
  else if ( strcasecmp( op, "setInt" ) == 0 ) {
    int32_t val = atoi( value.c_str() );
    bbc->set( BBname, name, ( int64_t )val );
  }
  else if ( strcasecmp( op, "getInt" ) == 0 ) {
    int64_t res = bbc->get( BBname, name );
    result = FwkStrCvt( res ).toString();
  }
  else if ( strcasecmp( op, "inc" ) == 0 ) {
    int64_t res = bbc->increment( BBname, name );
    result = FwkStrCvt( res ).toString();
  }
  else if ( strcasecmp( op, "dec" ) == 0 ) {
    int64_t res = bbc->decrement( BBname, name );
    result = FwkStrCvt( res ).toString();
  }
  else {
    FWKSEVERE( "Unsupported operation specified: \"" << op << "\", exiting." );
    retVal = -1;
  }
  if ( !result.empty() ) {
    std::cout << result;
  }

  delete bbc;

  return retVal;
}  /* main() */

