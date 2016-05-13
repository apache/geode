#include "ThinClientSSL.hpp"

DUNIT_MAIN
{
  // SSL CANNOT BE ENABLED ON SERVERS WITHOUT LOCATORS ENABLED
  
  //doThinClientSSL( false );
  
  doThinClientSSL( true, true );
  
  //doThinClientSSL( true, false );
}
END_MAIN
