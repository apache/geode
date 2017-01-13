#include "ThinClientVersionedOps.hpp"

DUNIT_MAIN
  {
    /*testServerGC, TestCase-3
     * Two clients, Two servers
     * One client connected to one server and another client connected to
     * another
     * server
     * both clients have registerAllKeys
     * Tombstone timeout on server reset to receive an early GC.
     * REPLICATE_PERSISTENT region. Destroy multiple keys, a GC is received and
     * it
     * should be handled properly.
     * For now, no error message is thrown. Log files need to be verified.
     */
    CALL_TASK(CreateServers_With_Locator_Disk);

    CALL_TASK(StartClient1);
    CALL_TASK(StartClient2);
    CALL_TASK(testServerGC);

    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServers_With_Locator);
  }
END_MAIN
