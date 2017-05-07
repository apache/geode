#include "ThinClientPutAllTimeout.hpp"

DUNIT_MAIN
{
    runThinClientPutAllTimeout( true, true );
    runThinClientPutAllTimeout ( true, false );
}
END_MAIN


