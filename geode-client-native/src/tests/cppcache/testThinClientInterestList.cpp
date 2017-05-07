#include "ThinClientInterestList.hpp"

DUNIT_MAIN
{
  runThinClientInterestList( false );
  runThinClientInterestList( true, true );
  runThinClientInterestList( true, false );
}
END_MAIN


