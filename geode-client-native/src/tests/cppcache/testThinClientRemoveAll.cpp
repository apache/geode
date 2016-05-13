#include "ThinClientRemoveAll.hpp"

DUNIT_MAIN
{
  runRemoveAll( true);
  runRemoveAll( false );
}
END_MAIN

DUNIT_MAIN
{
 runRemoveAllLocal();
}
END_MAIN
