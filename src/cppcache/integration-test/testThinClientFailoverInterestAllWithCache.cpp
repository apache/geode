#include "ThinClientFailoverInterestAllWithCache.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);

    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(InitializeClient1);
    CALL_TASK(InitializeClient2);

    CALL_TASK(VerifyClient1);
    CALL_TASK(VerifyClient2);

    CALL_TASK(CreateServer2_With_Locator_XML);

    CALL_TASK(CloseServer1);
    CALL_TASK(UpdateClient1);
    CALL_TASK(VerifyUpatesClient2);
    CALL_TASK(Client2UnregisterAllKeys);
    CALL_TASK(UpdateAndVerifyClient1);
    CALL_TASK(Client2VerifyOriginalValues);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer2);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
