#include "ThinClientDistOps2.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)
    CALL_TASK(CreateServer2And3_Locator)

    CALL_TASK(CreateClient1Regions_Pooled_Locator);
    CALL_TASK(CreateClient2Regions_Pooled_Locator);

    CALL_TASK(CreateClient1Entries);
    CALL_TASK(CreateClient2Entries);
    CALL_TASK(UpdateClient1Entry);
    CALL_TASK(UpdateClient2Entry);

    CALL_TASK(Client1GetAll);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
