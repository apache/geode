#include "ThinClientRegex2.hpp"

DUNIT_MAIN
{
  runThinClientRegex2( false);
  runThinClientRegex2( true, false);
  runThinClientRegex2( true, true);
}
END_MAIN


