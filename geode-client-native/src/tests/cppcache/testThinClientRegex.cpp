#include "ThinClientRegex.hpp"

DUNIT_MAIN
{
  runThinClientRegex( false);
  runThinClientRegex( true, true);
  runThinClientRegex( true, false);
}
END_MAIN
