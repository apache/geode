#include "ThinClientFailoverInterestAllWithCache.hpp"
DUNIT_MAIN
{
  runThinClientFailoverInterestAllWithCache( false );
  runThinClientFailoverInterestAllWithCache( true, true );
  runThinClientFailoverInterestAllWithCache( true, false );
}
END_MAIN
