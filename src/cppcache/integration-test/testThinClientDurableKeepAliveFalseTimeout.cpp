#include "ThinClientDurable.hpp"

DUNIT_MAIN
  {
    CALL_TASK(StartLocator);

    CALL_TASK(StartServersWithLocator);

    CALL_TASK(FeederInit);

    CALL_TASK(InitClient1Timeout30);
    CALL_TASK(InitClient2Timeout30);

    CALL_TASK(FeederUpdate1);

    // Verify that the clients receive the first set of events from feeder.
    CALL_TASK(VerifyFeederUpdate_1_C1);
    CALL_TASK(VerifyFeederUpdate_1_C2);

    CALL_TASK(CloseClient1KeepAliveFalse);
    CALL_TASK(CloseClient2KeepAliveFalse);

    CALL_TASK(FeederUpdate2);

    CALL_TASK(InitClient1DelayedStart);
    CALL_TASK(InitClient2Timeout30);

    CALL_TASK(VerifyClient1KeepAliveFalse);
    CALL_TASK(VerifyClient2KeepAliveFalse);

    CALL_TASK(CloseFeeder);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServers);

    CALL_TASK(CloseLocator);
  }
END_MAIN
