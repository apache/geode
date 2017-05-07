#include "ThinClientInterest2.hpp"

DUNIT_MAIN
{
  runThinClientInterest2( false );
  runThinClientInterest2( true, true );
  runThinClientInterest2( true, false );
}
END_MAIN


