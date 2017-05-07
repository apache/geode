#include "ThinClientListenerInit.hpp"

DUNIT_MAIN
{
  runListenerInit( false );
  runListenerInit( true, true );
  runListenerInit( true, false );
}
END_MAIN


