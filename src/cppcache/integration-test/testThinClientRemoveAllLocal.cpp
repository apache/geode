#include "ThinClientRemoveAll.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateClient1RegionsLocal);
    CALL_TASK(removeAllValidation);
    CALL_TASK(removeAllOpsLocal);
    CALL_TASK(CloseCache1);
  }
END_MAIN
