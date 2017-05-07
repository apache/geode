
#include "ThinClientDurableFailover.hpp"

DUNIT_MAIN
{
  doThinClientDurableFailover( false );
  doThinClientDurableFailover( true, true );
  doThinClientDurableFailover( true, false );
}
END_MAIN
