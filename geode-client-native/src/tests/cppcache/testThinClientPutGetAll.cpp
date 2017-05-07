#include "ThinClientPutGetAll.hpp"

DUNIT_MAIN
{
  runPutGetAll( true, true );
  runPutGetAll( true, false);
}
END_MAIN
