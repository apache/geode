#include "ThinClientPutAll.hpp"
DUNIT_MAIN
  {
    // runPutAll( false ); // removed as pdx object doesn't work with old
    // endpoint
    // scheme
    runPutAll();

    runPutAll1();

    runPutAll1(false);
  }
END_MAIN
