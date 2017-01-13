#include "ThinClientInterestList2.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML)
    CALL_TASK(CreateClient1Regions);
    CALL_TASK(CreateClient2Regions);

    CALL_TASK(CreateClient1Entries);
    CALL_TASK(CreateClient2Entries);
    CALL_TASK(UpdateClient1Entries);
    CALL_TASK(VerifyAndDestroyClient2Entries);
    CALL_TASK(VerifyDestruction);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
  }
END_MAIN
