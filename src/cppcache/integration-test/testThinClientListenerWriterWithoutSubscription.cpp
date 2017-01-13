#include "ThinClientListenerWriter.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML);

    CALL_TASK(SetupClient1_Pooled_Locator);
    CALL_TASK(SetupClient2_Pooled_Locator);
    CALL_TASK(Register2WithTrue);

    CALL_TASK(doOperations);
    CALL_TASK(validateListenerWriterWithNBSTrue);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);

    CALL_TASK(CreateServer1_With_Locator_NBSFalse);

    CALL_TASK(SetupClient1_Pooled_Locator);
    CALL_TASK(SetupClient2_Pooled_Locator);
    CALL_TASK(Register2WithFalse);
    CALL_TASK(SetupClient3_Pooled_Locator);
    CALL_TASK(Register3WithFalse);

    CALL_TASK(doOperations);
    CALL_TASK(validateListenerWriterWithNBSFalse);
    CALL_TASK(validateListenerWriterWithNBSFalseForClient3);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseCache3);
    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
