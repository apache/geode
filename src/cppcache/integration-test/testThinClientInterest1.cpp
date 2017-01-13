#include "ThinClientInterest1.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML)
    CALL_TASK(SetupClient1_Pool_Locator);
    CALL_TASK(populateServer);
    CALL_TASK(setupClient2_Pool_Locator);
    CALL_TASK(verify);
    CALL_TASK(StopClient1);
    CALL_TASK(StopClient2);
    CALL_TASK(StopServer);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
