#include "ThinClientInterest1.hpp"

DUNIT_MAIN
{
  runThinClientInterest1( false );
  runThinClientInterest1( true, true );
  runThinClientInterest1( true, false );
}
END_MAIN


