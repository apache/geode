#include "ThinClientNotification.hpp"

DUNIT_MAIN
{
  doThinClientNotification( false );
  doThinClientNotification( true, true );
  doThinClientNotification( true, false );
}
END_MAIN
