
#include "ThinClientDurableReconnect.hpp"

DUNIT_MAIN
{
  doThinClientDurableReconnect( false );
  doThinClientDurableReconnect( true );
  doThinClientDurableReconnect( true, false );
}
END_MAIN
