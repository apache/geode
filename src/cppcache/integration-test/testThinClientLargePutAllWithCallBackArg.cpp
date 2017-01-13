#include "ThinClientPutAllWithCallBack.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateServer1);
    CALL_TASK(CreateServer2);

    CALL_TASK(CreateClient1RegionsWithCachingWithConcurrencyCheck);
    CALL_TASK(CreateClient2Regions);

    CALL_TASK(RegisterClient1Keys);
    CALL_TASK(RegisterClient2Keys);

    CALL_TASK(VerifyInitialEntries);
    CALL_TASK(TriggerAfterUpdateEvents);
    CALL_TASK(VerifyUpdatedEntries);
    CALL_TASK(ExecuteLargePutAll);
    CALL_TASK(VerifyLargePutAll);
    CALL_TASK(VerifyRegionService);
    CALL_TASK(InvalidateKeys);
    CALL_TASK(VerifyPutAllWithLongKeyAndStringValue);
    CALL_TASK(VerifyPutAllWithLongKeyAndLongValue);
    CALL_TASK(VerifyPutAllWithObjectKey);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);
  }
END_MAIN
