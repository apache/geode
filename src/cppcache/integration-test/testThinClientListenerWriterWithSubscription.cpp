#include "ThinClientListenerWriter.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML_Bug849);

    CALL_TASK(SetupClient1withCachingEnabled_Pooled_Locator);
    CALL_TASK(SetupClient2withCachingEnabled_Pooled_Locator);

    CALL_TASK(RegisterKeys);
    CALL_TASK(doEventOperations);
    CALL_TASK(validateListenerWriterEventsWithNBSTrue);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
