#include "ThinClientCallbackArg.hpp"
DUNIT_MAIN
{
  runCallbackArg( false);
  runCallbackArg( true, true);
  runCallbackArg( true, false);
}
END_MAIN
