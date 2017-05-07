#include "ThinClientTXFailover.hpp"

DUNIT_MAIN
{
  runThinClientFailover( true, true );    // Locator
  runThinClientFailover( true, false );   // EP
  runThinClientFailover( true, true, true );    // Locator With Sticky
  runThinClientFailover( true, false, true );   // EP With Sticky
}
END_MAIN
