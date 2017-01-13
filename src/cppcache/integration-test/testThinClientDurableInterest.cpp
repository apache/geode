#include "ThinClientDurableInterest.hpp"

DUNIT_MAIN
  {
    CALL_TASK(StartLocator);

    startServers();

    CALL_TASK(FeederInit);
    CALL_TASK(Clnt1Init);
    CALL_TASK(Clnt2Init);
    CALL_TASK(FeederUpdate1);
    CALL_TASK(Clnt1Down);
    CALL_TASK(Clnt2Down);
    CALL_TASK(Clnt1Up);
    CALL_TASK(Clnt2Up);
    CALL_TASK(FeederUpdate2);
    CALL_TASK(ValidateClient1ListenerEventPayloads);
    CALL_TASK(ValidateClient2ListenerEventPayloads);
    CALL_TASK(CloseFeeder);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServer);

    closeLocator();
  }
END_MAIN
