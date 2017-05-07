/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifdef _WIN32
#pragma warning(disable:4244)
#pragma warning(disable:4996)
#endif 
#ifdef _WIN32
#define _AFXDLL
#include <afxpriv.h>
#include <mscoree.h>
#include <corhlpr.h>
#endif

#include "Client.hpp"
#include "fwklib/IpcHandler.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TimeBomb.hpp"
#include "fwklib/TestClient.hpp"

#include <map>

//extern "C" void MemRegisterTask( void );

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::perf;

bool doTasks = true;

// ----------------------------------------------------------------------------

#ifndef USE_SMARTHEAP
  #define USE_SMARTHEAP 0
#endif

#if USE_SMARTHEAP
extern "C" {
  void MemRegisterTask( );
}
#endif 

// ----------------------------------------------------------------------------

typedef std::map<std::string, ACE_DLL *> ContainerMap;

// ----------------------------------------------------------------------------

FwkAction getAction( std::string name, ACE_DLL * lib ) {
  FwkAction action = NULL;
  action = ( FwkAction )lib->symbol( name.c_str() );
  if ( action == NULL ) {
    FWKINFO( "Failed to find action " << name << ", error: " << lib->error() );
  }
  return action;
}

// ----------------------------------------------------------------------------

void runInitOrFin( const char * act, ACE_DLL * lib, const char * initArgs ) {
  FwkAction action = getAction( act, lib);
  if ( action != NULL ) {
    int32_t status = FWK_SUCCESS;
    // We will exit if task does not complete in time - 5 min.
    TimeBomb tb( 300, -71, "While trying to initialize/finalize test library." );
    if ( initArgs == NULL ) {
      status = ( *action )( "" );
    }
    else {
      status = ( *action )( initArgs );
    }
    tb.disarm();
    if ( status != FWK_SUCCESS ) {
      FWKSEVERE( "Library " << act << " returned status " << status );
    }
    else {
      FWKINFO( act << " of library succeeded." );
    }
  }
  else {
    FWKSEVERE( "Failed to " << act << " library." );
  }
}
  
// ----------------------------------------------------------------------------

ACE_DLL * loadLibrary( std::string& name, const char * args )
{
  ACE_DLL * lib = NULL;
  if ( name.empty() ) {
    FWKSEVERE( "No container specified." );
    return lib;
  }
  std::string libname = name;
#ifndef _WIN32
  std::string dll = "lib";
  dll.append( name );
  libname = dll;
#endif
  lib = new ACE_DLL( libname.c_str() );
  if ( lib == NULL ) { 
    FWKEXCEPTION( "Failed to find container " << name << " ( " << libname << " )" );
  }
  runInitOrFin( "initialize", lib, args );
  return lib;
}

std::string g_file;
std::string g_class;
std::string g_method;

#ifdef _WIN32

//void CheckFail( HRESULT hr, const char * msg ) {
//  if ( !SUCCEEDED( hr ) ) {
//    FWKEXCEPTION( msg << hr );
//  }
//}

ICLRRuntimeHost * g_clrHost = NULL;

#define CHECKFAIL( x, y ) if ( !SUCCEEDED( x ) ) { FWKEXCEPTION( y << x ); }

void checkCLR( bool unloadCLR ) {
  static bool clrLoaded = false;
  HRESULT result;
  
  if ( unloadCLR ) {
    if ( clrLoaded ) {
      clrLoaded = false;
      result = g_clrHost->Stop();
      CHECKFAIL( result, "Attempt to stop runtime failed: " );
      result = g_clrHost->Release();
      CHECKFAIL( result, "Attempt to release runtime failed: " );
      FWKINFO( "CLR stopped and released." );
    }
    return;
  }
  if ( ! clrLoaded ) {
    LPWSTR version = NULL; // Load the latest CLR version available
    LPWSTR gcType = L"wks"; // Workstation GC ("wks" or "svr" overrides)
    DWORD flags = 0; // No flags needed
    REFCLSID cid = CLSID_CLRRuntimeHost;
    REFIID riid = IID_ICLRRuntimeHost;
    result = 
      CorBindToRuntimeEx( version, gcType, flags, cid, riid, ( PVOID * )&g_clrHost );
   
    CHECKFAIL( result, "Bind to runtime failed: " );
  
    // Now, start the CLR.
    result = g_clrHost->Start();
    if ( result != S_FALSE ) {
      CHECKFAIL( result, "Runtime startup failed: ");
    }
    clrLoaded = true;
    FWKINFO( "CLR loaded and started." );
  }
}

int32_t runMethod( const char * args ) {
  USES_CONVERSION;
  int32_t retVal = FWK_SEVERE;
  try {
    char * fptr = NULL;
    char buff[4096];
    
    DWORD sres = SearchPath( NULL, g_file.c_str(), NULL, 4096, buff, &fptr );
    if ( sres <= 0 ) { // we did not find our file
      FWKSEVERE( "Failed to find assembly: " << g_file );
      return retVal;
    }
    FWKINFO( "Found our file: " << buff );
    // And execute the program...
    LPCWSTR path = A2W( buff );
    LPCWSTR typeName = A2W( g_class.c_str() );
    LPCWSTR methodName = A2W( g_method.c_str() );
    LPCWSTR arguments = A2W( args );
    DWORD res = 0;
    HRESULT result = g_clrHost->ExecuteInDefaultAppDomain( path, typeName, 
      methodName, arguments, &res );
    CHECKFAIL( result, "Execution of method failed: " );
    retVal = res;
  } catch( FwkException & ex ) {
      FWKSEVERE( "Test failed with exception: " << ex.getMessage() );
  }
  return retVal;
}

#else

int32_t runMethod( const char * args ) {
  return FWK_SEVERE;
}

void checkCLR( bool unloadCLR ) {}

#endif

std::string handleNetTask( std::string taskId, ContainerMap& libs, std::string& file,
                            std::string& className, std::string& action,
                            const char * initArgs, int32_t thrds ) {
  std::string status = "SUCCESS:: Task not run.";
  g_file = file;
  g_class = className;
  std::string fullName = file + "::" + className;
  ContainerMap::iterator entry  = libs.find( fullName );
  if ( entry == libs.end() ) {
    FWKINFO( "Using Assembly: " << file );
    libs[fullName] = NULL;
    // need to init library
    g_method = "initialize";
    int32_t res = runMethod( initArgs );
    if ( res != FWK_SUCCESS ) {
      FWKSEVERE( "Failed to initialize library." );
    }
  }
  if ( doTasks ) {
    status = "FAILURE";
    g_method = action;
    if ( thrds > 1 ) {
      TestClient * client = TestClient::getTestClient();
      int32_t failures = 0;
      ThreadedTask tt( runMethod, taskId );
      int32_t passes = client->runThreaded( &tt, thrds );
      failures = tt.getFailCount();
      if ( passes == thrds ) {
        status = "SUCCESS";
      }
    }
    else {
      int32_t res = runMethod( taskId.c_str() );
      if ( res == 0 ) {
        status = "SUCCESS";
      }
    }
  }
  return status;
}

std::string handleDllTask( std::string taskId, ContainerMap& libs, 
                            std::string& container, std::string& action,
                            const char * initArgs, int32_t thrds ) {
  std::string status = "SUCCESS:: Task not run.";
  ACE_DLL * lib = NULL;
  ContainerMap::iterator entry  = libs.find( container );
  if ( entry != libs.end() ) {
    FWKINFO( "Using library: " << container );
    lib = ( *entry ).second;
  }
  if ( lib == NULL ) {
    FWKINFO( "Loading library: " << container << "init args " << initArgs );
    lib = loadLibrary( container, initArgs );
    if ( lib == NULL ) {
      FWKSEVERE( "Unable to find container " << container << 
        " for task: " << taskId );
      status = "FAILURE :: Unable to find container ";
      status += container + " for task: ";
      status += taskId;
      return status;
    }
    libs[container] = lib;
  }
  FwkAction act = getAction( action, lib);
  if ( act == NULL ) {
    FWKSEVERE( "Unable to lookup action: " << action );
    status = "FAILURE:: Unable to lookup action: ";
    status += action;
    return status;
  }
  if ( doTasks ) {
    status = "FAILURE";
    if ( thrds > 1 ) {
      TestClient * client = TestClient::getTestClient();
      int32_t failures = 0;
      ThreadedTask tt( act, taskId );
      int32_t passes = client->runThreaded( &tt, thrds );
      failures = tt.getFailCount();
      if ( passes == thrds ) {
        status = "SUCCESS";
      }
    }
    else {
      int32_t retVal = ( *act )( taskId.c_str() );
      if ( retVal == 0 ) {
        status = "SUCCESS";
      }
    }
  }
  return status;
}

std::string handleTask( FwkTask * task, ContainerMap& libs, const char * initArgs ) {
  std::string status;
  std::string taskId = task->getTaskId();
  std::string container = task->getContainer();
  if ( container.empty() ) {
    FWKSEVERE( "Unable to find container for task: " << taskId );
    status = "FAILURE :: Unable to find container for task: ";
    status += taskId;
    return status;
  }
  std::string action = task->getAction();
  if ( action.empty() ) {
    FWKSEVERE( "Unable to find action for task: " << taskId );
    status = "FAILURE :: Unable to find action for task: ";
    status += taskId;
    return status;
  }
  // We will exit if task does not complete in time
  TimeBomb tb( task->getWaitTime(), -72, task->getTaskId() );
  // See if we need to run this task threaded
  int32_t cnt = task->getThreadCount();
  std::string className = task->getClass();
  if ( className.empty() ) {
    status = handleDllTask( taskId, libs, container, action, initArgs, cnt );
  }
  else {
    // Do we have the CLR loaded and started yet?
    checkCLR( false );
    status = handleNetTask( taskId, libs, container, className, action, initArgs, cnt );
  }
  tb.disarm();
  return status;
}
void finalizeLibs( ContainerMap& libs ) {
  FWKINFO( "Number of libraries to finalize: " << libs.size() );
  // iterate thru map calling finalize
  ContainerMap::iterator entry  = libs.begin();
  while ( entry != libs.end() ) {
    ACE_DLL * lib = (*entry).second;
    std::string fullName = (*entry).first;
    if ( lib != NULL ) {
      FWKINFO( "Attempting to finalize library " << fullName );
      runInitOrFin( "finalize", lib, NULL );
      FWKINFO( "Finalize library complete " << fullName );
    }
    else {
      uint32_t idx = static_cast<uint32_t> (fullName.find( "::" ));
      g_file = fullName.substr( 0, idx );
      g_class = "";
      if ( ( idx + 2 ) < fullName.size() ) {
        g_class = fullName.substr( idx + 2 );
      }
      g_method = "finalize";
      runMethod( " " );
    }
    entry++;
  }
  // iterate thru map deleting lib object
  entry  = libs.begin();
  while ( entry != libs.end() ) {
    ACE_DLL * lib = (*entry).second;
    std::string fullName = (*entry).first;
    if ( lib != NULL ) {
      FWKINFO( "Attempting to delete library " << fullName );
      delete lib;
      lib = NULL;
    }
    entry++;
  }
}

/**
* DESCRIPTION:
*
*   Main function for C++ test client program.
*/

int main( int32_t argc, char * argv[] )
{

#if USE_SMARTHEAP
  MemRegisterTask( );
#endif

  /* Process Command Line args */
  if ( argc < 5 ) {
    FWKSEVERE( "Usage " << argv[0] << " <id> <testFile> <ipaddr> <bb_ipaddr> [-N]" );
    exit(1);
  }
  
//  MemRegisterTask();
  
#if USE_SMARTHEAP
#else
  gemfire::SignalHandler::installBacktraceHandler();
#endif

  char * cid =  ACE_TEXT_ALWAYS_CHAR( argv[1] );
  int32_t clientId =  atoi( cid );
  char * testFile = ACE_TEXT_ALWAYS_CHAR( argv[2] );
  char * ipaddr = ACE_TEXT_ALWAYS_CHAR( argv[3] );
  char * bbaddr = ACE_TEXT_ALWAYS_CHAR( argv[4] );
  const char * noExec = "";
  if ( argc == 6 ) {
    noExec = ACE_TEXT_ALWAYS_CHAR( argv[5] );
  }
  doTasks = ( strcmp( noExec, "-N" ) == 0 ) ? false : true;

  // What host are we running on?
  char clientHost[128];
  *clientHost = 0;
  if ( ACE_OS::hostname( clientHost, 128 ) == -1 ) {
    FWKEXCEPTION( "Failed to resolve hostname." );
  }
  char * p = strchr( clientHost, '.' );
  if ( p != NULL )
    *p = 0;

  int32_t pid = ACE_OS::getpid();
  FWKINFO( "Client Id: " << clientId << ", process id: " << pid << 
    ", running on " << clientHost <<
    ", test xml: " << testFile << ", connecting to driver at: " <<
    ipaddr << ", and BB at: " << bbaddr << "." );

  // Load the test specification
  TestDriver * coll = NULL;
  try {
    coll = new TestDriver( testFile );
  } catch( ... ) {
    FWKSEVERE( "Failed to load test file: " << testFile << ", exiting." );
    return -1;
  }

  // Connect to the driver process
  ACE_INET_Addr driver( ipaddr );
  IpcHandler * ip = 0;
  try {
    ip = new IpcHandler( driver, 30 );
  } catch ( FwkException& ex ) {
    FWKSEVERE( "Caught exception: " << ex.what() );
    FWKSEVERE( "Client exiting with error." );
    return -1;
  }
  
  // Get the maxThreads from the collection
  int32_t thrds = 50;
  TestClient * client = TestClient::createTestClient( thrds, clientId );
  if ( client == NULL ) {
    FWKSEVERE( "Unable to instantiate client threads, exiting." );
    return -1;
  }

  int32_t tsport = driver.get_port_number() - 4;
  char initArgs[4096];
  sprintf( initArgs, "%s %d %s %d", testFile, clientId, bbaddr, tsport );
  std::string status;
  try {
    perf::logSize( "Client size prior to running tasks." );
    FWKINFO( "Client is initialized, ready to receive tasks." );
    // Wait for instructions from the driver.
    IpcMsg msg = IPC_NULL;
    ContainerMap libs;
    FwkTask * task = NULL;
    while ( ( msg != IPC_EXIT ) && ( msg != IPC_ERROR ) ) {
      std::string taskId;
      msg = ip->getIpcMsg( 10, taskId );
      if ( msg == IPC_NULL ) {
        perf::sleepSeconds( 1 );
        continue;
      }
      if ( msg == IPC_PING ) {
        continue;
      }
      status = "SUCCESS";
      if ( msg == IPC_RUN ) {
        task = ( FwkTask * )coll->getTaskById( taskId.c_str() );
        if ( task == NULL ) {
          FWKSEVERE( "Unable to find task: " << taskId );
          status = "FAILURE :: Unable to find task: ";
          status += taskId;
        }
        else {
          FWKINFO( "Client " << clientId << " received task: " <<  taskId );
          status = handleTask( task, libs, initArgs );
        } // task != null
        // send status back to driver
        ip->sendResult( ( char * )status.c_str() );
        FWKINFO( "TaskResult:: " << status
          << " :: Task: " << taskId << " : Client " << clientId );
      } // if IPC_RUN
    } // while 
    if ( msg == IPC_EXIT ) {
      FWKINFO( "Client has been told to exit." );
    }
    FWKINFO( "Client is stopping test threads." );
    client->destroyTestClient();
    finalizeLibs( libs );
    checkCLR( true );
  } catch( FwkException& ex ) {
    FWKSEVERE( "Caught exception: " << ex.what() );
    FWKSEVERE( "Client exiting with error." );
    return -1;
  }
  FWKINFO( "Client is clearing xml collection." );
  delete coll;
  perf::logSize( "At Client termination." );
  FWKINFO( "Client is closing communication with driver." );
  delete ip;
  FWKINFO( "Client is exiting normally." );
  return 0;
}  /* main() */

