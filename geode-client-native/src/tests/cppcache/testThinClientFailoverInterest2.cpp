#include "ThinClientFailoverInterest2.hpp"
DUNIT_MAIN
{
  runThinClientFailoverInterest2( false );
  runThinClientFailoverInterest2( true, true );
  runThinClientFailoverInterest2( true, true );
}
END_MAIN
