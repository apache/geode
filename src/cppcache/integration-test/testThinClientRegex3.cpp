#include "ThinClientRegex3.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML)

    CALL_TASK(CreateClient1Regions);
    CALL_TASK(CreateClient2Regions);

    CALL_TASK(RegisterMalformedRegex);
    CALL_TASK(RegisterEmptyNullAndNonExistentRegex);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
