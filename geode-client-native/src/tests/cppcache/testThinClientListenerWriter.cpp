#include "ThinClientListenerWriter.hpp"

DUNIT_MAIN
{
  runListenerWriter( true, true );
  runListenerWriter( true, false );

  runListenerWriterAndVerfiyEvents( true, true );
  runListenerWriterAndVerfiyEvents( true, false );
}
END_MAIN
