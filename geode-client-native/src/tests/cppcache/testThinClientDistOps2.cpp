#include "ThinClientDistOps2.hpp"

DUNIT_MAIN
{
  runDistOps2( false );
  runDistOps2( true, true );
  runDistOps2( true, false );
}
END_MAIN
