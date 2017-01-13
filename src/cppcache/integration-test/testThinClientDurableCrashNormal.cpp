#include "ThinClientDurable.hpp"

DUNIT_MAIN
  {
    CALL_TASK(StartLocator);

    CALL_TASK(StartServersWithLocator);

    CALL_TASK(FeederInit);

    CALL_TASK(InitClient1Timeout300);
    CALL_TASK(InitClient2Timeout300);

    CALL_TASK(FeederUpdate1);

    // Verify that the clients receive the first set of events from feeder.
    CALL_TASK(VerifyFeederUpdate_1_C1);
    CALL_TASK(VerifyFeederUpdate_1_C2);

    CALL_TASK(CrashClient1);
    CALL_TASK(CrashClient2);

    CALL_TASK(FeederUpdate2);

    CALL_TASK(InitClient1Timeout300);
    CALL_TASK(InitClient2Timeout300);

    CALL_TASK(VerifyClient1);
    CALL_TASK(VerifyClient2);

    CALL_TASK(CloseFeeder);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);

    CALL_TASK(CloseServers);

    CALL_TASK(CloseLocator);
  }
END_MAIN
