#include "ThinClientPutAllWithCallBack.hpp"
DUNIT_MAIN
{
  // runPutAll( false ); // removed as pdx object doesn't work with old endpoint scheme
  runPutAll( true, true );
  runPutAll( true, false );

  runPutAll1( true, true );
  runPutAll1( true, false );

  runPutAll1( true, true, false );
  runPutAll1( true, false, false );
}
END_MAIN
