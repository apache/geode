#include "ThinClientFailoverInterest.hpp"
DUNIT_MAIN
{
  runThinClientFailoverInterest( false );
  runThinClientFailoverInterest( true, false );
  runThinClientFailoverInterest( true, true );
}
END_MAIN

