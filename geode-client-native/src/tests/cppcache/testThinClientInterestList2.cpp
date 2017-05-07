#include "ThinClientInterestList2.hpp"

DUNIT_MAIN
{
  runThinClientInterestList2( false);
  runThinClientInterestList2( true, true);
  runThinClientInterestList2( true, false);
}
END_MAIN


