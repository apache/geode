#include "ThinClientDurable.hpp"

DUNIT_MAIN
  {
    startServers();

    CALL_TASK(FeederInit);

    CALL_TASK(InitClient1Timeout300);
    CALL_TASK(InitClient2Timeout300);

    CALL_TASK(FeederUpdate1);

    // Verify that the clients receive the first set of events from feeder.
    CALL_TASK(VerifyFeederUpdate_1_C1);
    CALL_TASK(VerifyFeederUpdate_1_C2);

    CALL_TASK(DisconnectClient1);
    CALL_TASK(DisconnectClient2);

    CALL_TASK(FeederUpdate2);

    CALL_TASK(ReviveClient1);
    CALL_TASK(ReviveClient2AndWait);

    CALL_TASK(VerifyClient1);
    CALL_TASK(VerifyClient2);

    CALL_TASK(CloseFeeder);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServers);

    closeLocator();
  }
END_MAIN
