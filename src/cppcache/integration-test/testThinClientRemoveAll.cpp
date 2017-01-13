#include "ThinClientRemoveAll.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(CreateServer2_With_Locator_XML);
    CALL_TASK(CreateClient1Regions_Pooled_Locator);
    CALL_TASK(CreateClient2Regions_Pooled_Locator);

    CALL_TASK(removeAllValidation);
    CALL_TASK(removeAllOps);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
