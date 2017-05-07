#include "ThinClientDurableInterest.hpp"

DUNIT_MAIN
{
  doThinClientDurableInterest( false );
  doThinClientDurableInterest( true );
  doThinClientDurableInterest( true, false );

}
END_MAIN
