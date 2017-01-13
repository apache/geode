#include "ThinClientPutAllWithCallBack.hpp"
DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(CreateServer2_With_Locator_XML);

    CALL_TASK(CreateClient1RegionsWithCachingWithConcurrencyCheck);

    CALL_TASK(PutAllOps);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
