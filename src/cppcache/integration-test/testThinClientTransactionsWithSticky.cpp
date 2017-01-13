#include "ThinClientTransactions.hpp"

DUNIT_MAIN
  {
    CALL_TASK(Alter_Client_Grid_Property_1);
    CALL_TASK(Alter_Client_Grid_Property_2);

    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)

    CALL_TASK(CreateNonexistentServerRegion_Pooled_Locator_Sticky);
    CALL_TASK(CreateClient1PooledRegionWithSticky);
    CALL_TASK(CreateClient2PooledRegionWithSticky);

    CALL_TASK(CreateClient1Entries);
    CALL_TASK(CreateClient2Entries);
    CALL_TASK(UpdateClient1Entries);
    CALL_TASK(UpdateClient2Entries);
    CALL_TASK(CreateClient1EntryTwice);

    CALL_TASK(CreateClient1KeyThriceWithSticky);

    CALL_TASK(SuspendResumeInThread);
    CALL_TASK(SuspendResumeCommit);
    CALL_TASK(SuspendResumeRollback);
    CALL_TASK(SuspendTimeOut);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
