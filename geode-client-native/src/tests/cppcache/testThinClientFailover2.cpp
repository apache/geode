#include "ThinClientFailover2.hpp"
DUNIT_MAIN
{
  //runThinClientFailover2( false );
  runThinClientFailover2( true, true );
  runThinClientFailover2( true, false );
}
END_MAIN

