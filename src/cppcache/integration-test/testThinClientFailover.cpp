#include "ThinClientFailover.hpp"

DUNIT_MAIN
  {
    runThinClientFailover();
    runThinClientFailover(true);
  }
END_MAIN
