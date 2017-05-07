#include "ThinClientInterest3Cacheless.hpp"

DUNIT_MAIN
{
  testThinClientInterest3Cacheless( false );
  testThinClientInterest3Cacheless( true, true );
  testThinClientInterest3Cacheless( true, false );
}
END_MAIN

