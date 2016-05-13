#include "ThinClientDurable.hpp"

DUNIT_MAIN
{
  doThinClientDurable( false );
  doThinClientDurable( true );
  doThinClientDurable( true, false );
}
END_MAIN
