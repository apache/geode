#include "ThinClientVersionedOps.hpp"

DUNIT_MAIN
  {
    /*TestCase-1
     * Two clients, Two servers
     * One client connected to one server and another client connected to
     * another
     * server
     * both clients have registerAllKeys
     * Run various operations and check if the values across the clients and
     * server are same.
     */

    CALL_TASK(CreateServers_With_Locator);
    CALL_TASK(StartClient1);
    CALL_TASK(StartClient2);

    CALL_TASK(PutOnClient1);
    CALL_TASK(PutOnClient2);
    CALL_TASK(GetOnClient1);
    CALL_TASK(GetOnClient2);

    // test transaction
    CALL_TASK(threadPutonClient1);
    CALL_TASK(transactionPutOnClient2);
    CALL_TASK(verifyGetonClient1);

    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServers_With_Locator);
  }
END_MAIN
