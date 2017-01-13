#include "ThinClientListenerInit.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)
    CALL_TASK(SetupClient_Pooled_Locator);
    CALL_TASK(InitClientEvents);
    CALL_TASK(testLoaderAndWriter);
    CALL_TASK(testCreatesAndUpdates)
    CALL_TASK(testDestroy);
    CALL_TASK(CloseCache1);
    CALL_TASK(StopServer)
    CALL_TASK(CloseLocator1);
  }
END_MAIN
