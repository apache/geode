#include "ThinClientInterest3.hpp"

DUNIT_MAIN
{
  runThinClientInterest3( false );
  runThinClientInterest3( true, true );
  runThinClientInterest3( true, false );
}
END_MAIN


