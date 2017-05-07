#include "ThinClientFailover3.hpp"
DUNIT_MAIN
{
  runThinClientFailover3( false );
  runThinClientFailover3( true, true );
  runThinClientFailover3( true, false );
}
END_MAIN


