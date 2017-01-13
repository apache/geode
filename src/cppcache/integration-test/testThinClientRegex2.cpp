#include "ThinClientRegex2.hpp"

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML)

    CALL_TASK(StepOne_Pool_Locator);
    CALL_TASK(StepTwo_Pool_Locator);

    CALL_TASK(StepThree);
    CALL_TASK(StepFour);
    CALL_TASK(StepFive);
    CALL_TASK(StepSix);
    CALL_TASK(StepSeven);
    CALL_TASK(StepEight);
    CALL_TASK(StepNine);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
