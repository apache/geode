#include "ThinClientFailover3.hpp"
DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)

    CALL_TASK(SetupClient1PooledLocator);
    CALL_TASK(SetupClient2PooledLocator);

    CALL_TASK(Client1CreateEntries);
    CALL_TASK(Client2CreateEntries);

    CALL_TASK(CreateServer2_With_Locator);

    CALL_TASK(CloseServer1);  // FailOver
    CALL_TASK(Client1UpdateEntries);
    CALL_TASK(Client2UpdateEntries);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);

    CALL_TASK(CloseServer2);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
