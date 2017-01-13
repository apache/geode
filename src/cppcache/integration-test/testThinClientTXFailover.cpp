#include "ThinClientTXFailover.hpp"

DUNIT_MAIN
  {
    runThinClientFailover();
    runThinClientFailover(true);  // With Sticky
  }
END_MAIN
