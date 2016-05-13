#include "ThinClientRegex3.hpp"

DUNIT_MAIN
{
  runThinClientRegex3( false );
  runThinClientRegex3( true, true );
  runThinClientRegex3( true, false );
}
END_MAIN
