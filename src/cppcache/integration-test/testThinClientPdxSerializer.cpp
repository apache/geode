#include "ThinClientPdxSerializer.hpp"

DUNIT_MAIN
  {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator3)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)

    CALL_TASK(putFromVersion1_PS)

    CALL_TASK(putFromVersion2_PS)

    CALL_TASK(getputFromVersion1_PS)

    CALL_TASK(getAtVersion2_PS)

    CALL_TASK(CloseCache1)
    CALL_TASK(CloseCache2)
    CALL_TASK(CloseServer)

    CALL_TASK(CloseLocator)
  }
END_MAIN
