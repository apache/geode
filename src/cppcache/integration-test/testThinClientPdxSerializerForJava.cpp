#include "ThinClientPdxSerializer.hpp"

DUNIT_MAIN
  {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator2)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)

    CALL_TASK(JavaPutGet)  // c1
    CALL_TASK(JavaGet)     // c2

    CALL_TASK(CloseCache1)
    CALL_TASK(CloseCache2)
    CALL_TASK(CloseServer)

    CALL_TASK(CloseLocator)
  }
END_MAIN
