#include "ThinClientFailoverRegex.hpp"
DUNIT_MAIN
{
  runThinClientFailOverRegex( false);
  runThinClientFailOverRegex( true, true);
  runThinClientFailOverRegex( true, false);
}
END_MAIN

