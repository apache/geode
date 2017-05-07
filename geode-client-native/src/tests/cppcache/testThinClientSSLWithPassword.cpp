#include "ThinClientSSLWithPassword.hpp"

DUNIT_MAIN
{
	// SSL CANNOT BE ENABLED ON SERVERS WITHOUT LOCATORS ENABLED

	//doThinClientSSL( false );

	doThinClientSSLWithPassword( true, true);  

	//doThinClientSSL( true, false );
} 
END_MAIN
