/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "Driver.hpp"
#include "fwklib/IpcHandler.hpp"
#include "fwklib/TaskClient.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/FwkBBServer.hpp"
#include "fwklib/FwkStrCvt.hpp"
#include "fwklib/TimeLimit.hpp"
#include "fwklib/TimeBomb.hpp"
#include "fwklib/TimeSync.hpp"
#include "fwklib/UDPIpc.hpp"

//extern "C" void MemRegisterTask( void );

#include <ace/OS.h>
#include <ace/Addr.h>
#include <ace/Unbounded_Set.h>
#include <ace/SOCK_Acceptor.h>

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::perf;

#include <string>
#include <list>
#include <map>
#include <vector>
using std::string;

typedef std::list<string> StdStringList;
typedef std::vector<TaskClient *> TaskClientVector;
typedef TaskClientVector::iterator TaskClientIter;
typedef std::vector<FwkTask *> FwkTaskVector;

uint32_t g_testsRun = 0;
uint32_t g_tasksRun = 0;
uint32_t g_passCount = 0;
uint32_t g_failCount = 0;
uint32_t g_runCount = 0;
uint32_t g_clientNoExit = 0;

FwkBBServer* g_bbServer = NULL;

string getTimeString( int32_t seconds ) {
  string result;
  const int32_t perMinute = 60;
  const int32_t perHour = 3600;
  const int32_t perDay = 86400;

  if ( seconds <= 0 ) {
    result += "No limit specified.";
    return result;
  }

  int32_t val = 0;
  if ( seconds > perDay ) {
    val = seconds / perDay;
    if ( val > 0 ) {
      result += FwkStrCvt( val ).toString();
      if ( val == 1 )
        result += " day ";
      else
        result += " days ";
    }
    seconds %= perDay;
  }
  if ( seconds > perHour ) {
    val = seconds / perHour;
    if ( val > 0 ) {
      result += FwkStrCvt( val ).toString();
      if ( val == 1 )
        result += " hour ";
      else
        result += " hours ";
    }
    seconds %= perHour;
  }
  if ( seconds > perMinute ) {
    val = seconds / perMinute;
    if ( val > 0 ) {
      result += FwkStrCvt( val ).toString();
      if ( val == 1 )
        result += " minute ";
      else
        result += " minutes ";
    }
    seconds %= perMinute;
  }
  if ( seconds > 0 ) {
      result += FwkStrCvt( seconds ).toString();
      if ( seconds == 1 )
        result += " second ";
      else
        result += " seconds ";
  }
  return result;
}

typedef std::list < std::string > HostOrder;
typedef std::map < std::string, int32_t > HostCounts;
typedef std::map < std::string, StdStringList * > HostMap;

HostOrder g_order;
HostMap g_hosts;
HostCounts g_counts;
int32_t totHosts = 0;

StdStringList * loadGroup( const std::string & grp ) {
  StdStringList * group = new StdStringList;
  if ( grp.empty() ) {
    FWKEXCEPTION( "Tried to load an unnamed host group." );
  }

  char groupFileName[128];
  FILE * fil;

  sprintf( groupFileName, "../hosts/%s", grp.c_str() );
  fil = fopen( groupFileName, "r" ); /* read only */
  if ( fil != ( FILE * )0 ) {
    char rbuff[1024];
    int32_t cnt = fscanf( fil, "%s", rbuff );
    while ( cnt > 0 ) {
      std::string hst = rbuff;
      group->push_back( hst );
      totHosts++;
      g_order.push_back( hst );
      HostCounts::iterator hciter = g_counts.find( hst );
      if ( hciter == g_counts.end() ) {
        g_counts[ hst ] = 0;
      }
      else {
        ( *hciter ).second--;
      }
      cnt = fscanf( fil, "%s", rbuff );
    }
    fclose( fil );
  }

  return group;
}

void loadAllGroups() {
  ACE_DIR * dir = ACE_OS::opendir( "../hosts" );
  if ( dir == NULL ) {
    FWKEXCEPTION( "No hosts provided for starting client processes." );
  }

  StdStringList * group = NULL;
  ACE_DIRENT * dent = ACE_OS::readdir( dir );
  while ( dent != NULL ) {
    std::string grp = dent->d_name;
    if ( ( grp != "." ) && ( grp != ".." ) ) {
      group = loadGroup( grp );
      g_hosts[ grp ] = group;
    }
    dent = ACE_OS::readdir( dir );
  }
}

void removeHost( std::string hst ) {
  HostOrder::iterator oiter = g_order.begin();
  while ( oiter != g_order.end() ) {
    if ( ( *oiter ) == hst ) {
      HostOrder::iterator prev = oiter;
      prev--;
      g_order.erase( oiter );
      oiter = prev;
    }
    oiter++;
  }
  HostCounts::iterator hciter = g_counts.find( hst );
  if ( hciter != g_counts.end() ) {
    g_counts.erase( hciter );
  }
  HostMap::iterator iter = g_hosts.begin();
  while ( iter != g_hosts.end() ) {
    StdStringList * group = ( *iter ).second;
    std::string grp = ( *iter ).first;
    StdStringList::iterator siter = group->begin();
    while ( siter != group->end() ) {
      if ( *siter == hst ) {
        totHosts--;
        StdStringList::iterator next = siter;
        next--;
        group->erase( siter );
        siter = next;
      }
      siter++;
    }
    iter++;
  }
}

// Find a host that has not been assigned to clients
std::string getUnusedHost() {
  std::string hst;
  for ( uint32_t limit = 0; limit < g_order.size(); limit++ ) {
    hst = g_order.front();
    g_order.pop_front();
    g_order.push_back( hst );
    HostCounts::iterator hciter = g_counts.find( hst );
    if ( hciter != g_counts.end() ) {
      int32_t ccnt = ( *hciter ).second;
      if ( ccnt <= 0 ) {
        hst = ( *hciter ).first;
        ( *hciter ).second++;
        return hst;
      }
    }
  }
  return hst;
}

std::string getNextHost( const std::string & grp ) {
  if ( grp == "*" ) {
    return getUnusedHost();
  }

  StdStringList * group = NULL;
  HostMap::iterator iter = g_hosts.find( grp );
  if ( iter == g_hosts.end() ) {
    group = loadGroup( grp );
    if ( group->size() > 0 ) {
      g_hosts[ grp ] = group;
    }
    else {
      if ( grp == FwkClientSet::m_defaultGroup ) {
        FWKEXCEPTION( "No hosts defined for default group, but default group is needed. Unable to start clients." );
      }
      FWKWARN( "No hosts defined for group " << grp << ", using host from group " << FwkClientSet::m_defaultGroup );
      return getNextHost( FwkClientSet::m_defaultGroup );
    }
  }
  else {
    group = ( *iter ).second;
  }
  std::string hst = group->front();
  group->pop_front();
  group->push_back( hst );
  HostCounts::iterator hciter = g_counts.find( hst );
  if ( hciter != g_counts.end() ) {
    ( *hciter ).second++;
  }
  return hst;
}

// Count how many hosts have not been assigned to clients
int32_t remainingHosts() {
  int32_t cnt = 0;

  HostCounts::iterator hciter = g_counts.begin();
  while ( hciter != g_counts.end() ) {
    int32_t ccnt = ( *hciter ).second;
    std::string name = ( *hciter ).first;
    while ( ccnt <= 0 ) {
      cnt++;
      ccnt++;
    }
    hciter++;
  }

  return cnt;
}

int32_t assignHostAndStartClient( TaskClient * client ) {
  int32_t result = 2;
  std::string grp = client->getHostGroup();
  while ( result == 2 ) {
    std::string host = getNextHost( grp );
    if ( host.empty() ) {
      if ( grp != "*" ) { // Group onther than "remaining hosts" required
        result = -1;
      }
      else {
        result = 1;
      }
    }
    else {
      client->setHost( host.c_str() );
      try {
        client->start();
        result = 0;
      } catch( FwkException & ex ) {
        FWKSEVERE( "Failed to start client on " << host << " dropping it from host list. " << ex.getMessage() );
        removeHost( host );
        result = 2;
      }
    }
  }
  return result;
}

TaskClientVector * startClients( TestDriver& coll, int32_t port,
  const char * ldir, const char * tfile, ACE_SOCK_Acceptor * listener ) {

  loadAllGroups();

  TaskClientVector * clients = new TaskClientVector();
  FwkClientSet * clnts = coll.getClients();
  int32_t ccnt = 0;
  FwkClient * remain = NULL;
  FwkClient * cur = ( FwkClient * )clnts->getFirst();
  if ( cur == NULL ) {
    remain = new FwkClient( "Default" );
    remain->setRemaining( true );
  }

  while ( cur != NULL ) {
    if ( cur->getRemaining() ) {
      if ( remain != NULL ) {
        FWKEXCEPTION( "Too many clients wish to use remaining hosts. Both " << remain->getName() << " and " << cur->getName() );
      }
      remain = cur;
    }
    else {
      std::string nam = cur->getName();
      const char * program = cur->getProgram();
      const char * arguments = cur->getArguments();

      if ( nam.size() == 0 ) {
        nam = " ";
      }

      TaskClient * client = NULL;
      if ( ( program == NULL ) || ( strlen( program ) == 0 ) ) {
        client = new TaskClient( port, ldir, tfile, listener, ccnt + 1, nam.c_str() );
      }
      else {
        client = new TaskClient( ldir, ccnt + 1, program, arguments, nam.c_str() );
      }
      std::string cgrp = cur->getHostGroup();
      client->setHostGroup( cgrp );
      int32_t result = assignHostAndStartClient( client );
      switch( result ) {
        case -1: // Can't run test
          FWKEXCEPTION( "Unable to find host for client: " << client->getName() << " unable to run test." );
        break;
        case 1: // Can't use client, drop it
          FWKSEVERE( "Unable to find host for client: " << client->getName() << " dropping it from client list." );
        break;
        default: // success
          ccnt++;
          clients->push_back( client );
        break;
      }
    }
    cur = ( FwkClient * )clnts->getNext( cur );
  }

  if ( remain != NULL ) {
    int32_t rcnt = remainingHosts();
    if ( rcnt == 0 ) {
      FWKEXCEPTION( "A client is specified to use remaining hosts, but all hosts have been used." );
    }

    std::string nam = remain->getName();
    const char * program = remain->getProgram();
    const char * arguments = remain->getArguments();
    std::string grp = "*";
    if ( nam.size() == 0 ) {
      nam = " ";
    }

    for ( int32_t i = 0; i < rcnt; i++ ) {
      TaskClient * client = NULL;
      if ( ( program == NULL ) || ( strlen( program ) == 0 ) ) {
        client = new TaskClient( port, ldir, tfile, listener, ccnt + 1, nam.c_str() );
      }
      else {
        client = new TaskClient( ldir, ccnt + 1, program, arguments, nam.c_str() );
      }
      client->setHostGroup( grp );
      int32_t result = assignHostAndStartClient( client );
      switch( result ) {
        case -1: // Can't run test
          FWKEXCEPTION( "Unable to find host for client: " << client->getName() << " unable to run test." );
        break;
        case 1: // Can't use client, drop it
          FWKSEVERE( "Unable to find host for client: " << client->getName() << " dropping it from client list." );
        break;
        default: // success
          ccnt++;
          clients->push_back( client );
        break;
      }
    }
  }

  FWKINFO( "Started " << clients->size() << " clients on " << totHosts << " hosts." );
  return clients;
}

ACE_Time_Value getTimeLimit( int32_t seconds ) {
  if ( seconds <= 0 ) {
    seconds = 9000000; // if not set, run forever ( essentially )
  }
  ACE_Time_Value wait( seconds );
  ACE_Time_Value now =  ACE_OS::gettimeofday();
  return now + wait;
}

bool checkTimeLimit( ACE_Time_Value limit ) {
  ACE_Time_Value now =  ACE_OS::gettimeofday();
  return now > limit;
}


int32_t getClientIdx( std::string& nam, TaskClientVector * clients ) {
  for ( int32_t j = 0; j < ( int32_t )clients->size(); j++ ) {
    if ( nam == (clients->at( j ))->getName() ) {
      return j;
    }
  }
  FWKSEVERE( "Did not find " << nam << " in the set of clients." );
  return -1;
}

void assignClients( FwkTask * testTask, TaskClientVector * clients ) {
  TaskClientIdxList * tcList = testTask->getTaskClients();
  FwkClientSet * clntSet = testTask->getClientSet();
  bool isExclusionSet = false;

  if ( clntSet != NULL ) {
    isExclusionSet = clntSet->getExclude();
  }

  if ( ( clntSet == NULL ) || isExclusionSet ) {
    // add all the unassigned clients
    for ( uint32_t j = 0; j < clients->size(); j++ ) {
      TaskClient * tc = clients->at( j );
      if ( !tc->isAssigned() && tc->runsTasks() ) {
        tcList->push_back( j );
      }
    }
  }

  if ( clntSet != NULL ) {
    // Loop thru clients
    const FwkClient * clnt = clntSet->getFirst();
    while ( clnt != NULL ) {
      if ( clnt->getProgram() == NULL ) {
        std::string nam = clnt->getName();
        int32_t idx = getClientIdx( nam, clients );
        if ( idx != -1 ) {
          if ( isExclusionSet ) { // remove members of set from tcList
            tcList->remove( ( uint32_t )idx );
          }
          else { // add members of set to tcList
            tcList->push_back( ( uint32_t )idx );
          }
        }
      }
      clnt = clntSet->getNext( clnt );
    }
  }

  if ( testTask->getTimesToRun() == 0 ) {
    testTask->setTimesToRun( static_cast<uint32_t>(tcList->size()) );
  }
}

int32_t startTasks( FwkTaskSet& tasks, TaskClientVector * clients, uint32_t * running, uint32_t errs ) {
  int32_t remaining = 0;
  int32_t taskRemaining = 0;
  bool hadErrors = false;

  // loop thru tasks
  FwkTask * task = ( FwkTask * )tasks.getFirst();
  while ( task != NULL ) {
    uint32_t run = task->getTimesToRun();
    uint32_t ran = task->getTimesRan();
    taskRemaining = ( ( run > ran ) ? ( run - ran ) : 0 );
    // loop thru clients for this task
    TaskClientIdxList * tcList = task->getTaskClients();
    if ( tcList->size() == 0 ) {
      taskRemaining = 0;
    }

    std::list<uint32_t>::iterator iter;
    for ( iter = tcList->begin(); iter != tcList->end(); iter++ ) {
      if ( taskRemaining == 0 ) {
        break;
      }
      if ( !task->continueOnError() && ( errs < g_failCount ) ) {
        task->setTimesToRun( task->getTimesRan() );
        hadErrors = true;
        break;
      }
      uint32_t idx = *iter;
      TaskClient * clnt = clients->at( idx );
      if ( clnt->isBusy() || !clnt->runsTasks() ) {
        continue;
      }
      try {
        if ( clnt->sendTask( ( char * )task->getTaskId().c_str() ) ) {
          clnt->setBusy( true );
          task->incTimesRan();
          (*running)++;
          g_runCount++;
          FWKINFO( "Task " << task->getTaskId() << " assigned to client "
            << clnt->getId() << "   " << clnt->getName() );
        }
        else {
          FWKEXCEPTION( "Failed to assign Task " << task->getTaskId() << " to client "
            << clnt->getId() << "   " << clnt->getName() );
        }
      } catch ( FwkException& ex ) {
        FWKSEVERE( "Caught exception sending task to client " << clnt->getId() << ": " << ex.what() );
        // Assume client dead, restart it
        FWKSEVERE( "Restarting client: " << clnt->getId() );
        clnt->kill();
        perf::sleepSeconds( 1 );
        clnt->start();
        clnt->setBusy( false );
      }
      // add to remaining
      ran = task->getTimesRan();
      taskRemaining--;
    }
    remaining += taskRemaining;
    task = ( FwkTask * )tasks.getNext( task );
  }
  if ( hadErrors )
    remaining = remaining * -1;
  return remaining;
}

uint32_t pollClients( TaskClientVector * clients ) {
  uint32_t done = 0;
  uint32_t running = 0;
  static uint32_t lastTime = 0;
  std::string who = " clients still running: ";

  for ( uint32_t i = 0; i < clients->size(); i++ ) {
    TaskClient * clnt = clients->at( i );
    if ( !clnt->isBusy() || !clnt->runsTasks() ) {
      continue;
    }
    try {
      std::string result;
      IpcMsg msg = clnt->getIpcMsg( 1, result );
      if ( msg == IPC_DONE ) {
        FWKINFO( "TaskResult:: " << result << " ::  Task " <<
          clnt->getTaskId() << " on client " << clnt->getId() );
        if ( result.find( "SUCCESS", 0 ) == 0 ) {
          g_passCount++;
        }
        else {
          g_failCount++;
        }
        clnt->setBusy( false );
        done++;
      }
      else {
        running++;
        char buf[16];
        sprintf( buf, "%d ", clnt->getId() );
        who.append( buf );
      }
    } catch ( FwkException& ex ) {
      char idBuf[20];
      sprintf(idBuf, "%d", clnt->getId());
      int64_t clientStatus = g_bbServer->get(CLIENT_STATUS_BB, idBuf);
      if (clientStatus == 1) {
        g_bbServer->set(CLIENT_STATUS_BB, idBuf, 0);
        FWKINFO("Client: " << clnt->getId() <<
            " expected exit while running task.");
        // Assume client dead, restart it
        FWKINFO("Restarting client: " << clnt->getId());
        std::string taskId =  clnt->getTaskId();
        clnt->kill();
        perf::sleepSeconds(1);
        clnt->start();
        clnt->setBusy(false);
        // Resend the task to the client
        if (clnt->sendTask((char*)taskId.c_str())) {
          clnt->setBusy(true);
          FWKINFO("Task " << taskId << " re-assigned to client "
              << clnt->getId() << "   " << clnt->getName());
        }
        else {
          FWKSEVERE("Failed to assign Task " << taskId <<
              " to client " << clnt->getId() << "   " << clnt->getName());
          // Assume client dead, restart it
          FWKSEVERE("Restarting client: " << clnt->getId());
          clnt->kill();
          perf::sleepSeconds(1);
          clnt->start();
          clnt->setBusy(false);
        }
      }
      else {
        FWKSEVERE( "Caught exception polling clients: " << ex.what() );
        FWKSEVERE( "Client: " << clnt->getId() << " exited while running task." );
        FWKINFO( "TaskResult:: FAILURE ::  Task on client " << clnt->getId() <<
            " failed due to client exiting." );
        // Assume client dead, restart it
        FWKSEVERE( "Restarting client: " << clnt->getId() );
        clnt->kill();
        perf::sleepSeconds(1);
        clnt->start();
        clnt->setBusy(false);
        g_failCount++;
        done++;
      }
    }
  }
  if ( ( running > 0 ) && ( running != lastTime ) ) {
    FWKINFO( running << who );
  }
  lastTime = running;
  return done;
}

typedef ACE_Unbounded_Set< std::string > StringSet;

void stopClients( TaskClientVector * clients ) {
  g_clientNoExit = static_cast<uint32_t>(clients->size());
  // Tell all clients to exit
  TaskClientIter iter = clients->begin();
  TaskClient * clnt = NULL;
  while ( iter != clients->end() ) {
    clnt = *iter;
    try {
      if ( clnt->runsTasks() ) {
        clnt->sendExit();
      }
    } catch( FwkException& ex ) {
      FWKINFO( "Caught " << ex.what()
        << " while attempting to communicate with client " << clnt->getId() );
      clnt->kill();
    }
    iter++;
  }

  int32_t maxWait = 300; // wait up to 5 minutes for clients to exit
  char * useVal = ACE_OS::getenv( "UseValgrind" );
  if ( ( useVal != NULL ) && ( *useVal == '1' ) ) {
    maxWait = 900; // valgrind slows things down, plus the valgrind logging at exit
  }

  TimeLimit patience( maxWait );
  int32_t cnt = 0;
  uint32_t exitCnt = 0;
  // Wait for clients to exit under their own power
  clnt = NULL;
  iter = clients->begin();
  while ( ( clients->size() > exitCnt ) && !patience.limitExceeded() ) {
    if ( iter == clients->end() ) {
      iter = clients->begin();
      if ( iter == clients->end() ) {
        continue;
      }
    }
    cnt++;
    if ( clients->size() && ( ( cnt % clients->size() ) == 0 ) ) {
      FWKINFO( "Checking for clients exit, " << clients->size() << " clients still running." );
      perf::sleepSeconds( 10 );
    }
    clnt = *iter;
    try {
      // Check for exiting message from client
      IpcMsg msg = clnt->getIpcMsg( 1 );
      if ( msg == IPC_EXITING ) {
        g_clientNoExit--;
        exitCnt++;
      }
    } catch( FwkException& ex ) {
      FWKINFO( "Caught " << ex.what()
        << " while attempting to communicate with client " << clnt->getId() );
      exitCnt++;
    } catch( ... ) {
      FWKINFO( "Caught unknown exception while attempting to communicate with client " << clnt->getId() );
      exitCnt++;
    }
    clnt = NULL;
    iter++;
  }
  if ( exitCnt == clients->size() ) {
    FWKINFO( "All clients are exiting." );
  }
  else {
    FWKINFO( "All clients are exiting except " << ( clients->size() - exitCnt ) );
  }

  StringSet hset;
  iter = clients->begin();
  while ( iter != clients->end() ) {
    clnt = *iter;
    std::string hst = clnt->getHost();
    hset.insert( hst );
    iter++;
  }
}

int32_t stopBusyClients( TaskClientVector * clients ) {
  int32_t fcnt = 0;
  try {
    for ( uint32_t i = 0; i < clients->size(); i++ ) {
      TaskClient * clnt = clients->at(i);
      if ( ( clnt->runsTasks() ) && ( clnt->isBusy() ) ) {
        clnt->sendExit();
        fcnt++;
      }
    }
    char * useVal = ACE_OS::getenv( "UseValgrind" );
    if ( ( useVal != NULL ) && ( *useVal == '1' ) ) {
      perf::sleepSeconds( 20 );
    }
    else {
      perf::sleepSeconds( 5 );
    }
    for ( uint32_t i = 0; i < clients->size(); i++ ) {
      TaskClient * clnt = clients->at(i);
      if ( ( clnt->runsTasks() ) && ( clnt->isBusy() ) ) {
        clnt->kill();
      }
    }
    if ( ( useVal != NULL ) && ( *useVal == '1' ) ) {
      perf::sleepSeconds( 20 );
    }
    else {
      perf::sleepSeconds( 5 );
    }
    for ( uint32_t i = 0; i < clients->size(); i++ ) {
      TaskClient * clnt = clients->at( i );
      if ( clnt->runsTasks() ) {
        clnt->start();
      }
    }
  } catch ( FwkException& ex ) {
    FWKSEVERE( "Driver caught exception: " << ex.what() );
  }
  return fcnt;
}

void clearClients( TaskClientVector * clients ) {
  for ( uint32_t i = 0; i < clients->size(); i++ ) {
    (clients->at( i ) )->setBusy( false );
  }
}

bool runGroup( FwkTaskSet& group, TaskClientVector * clients, ACE_Time_Value& wait ) {
  int32_t remainingRuns = 1;
  uint32_t running = 0;
  uint32_t errs = g_failCount;
  bool failTest = false;
  int32_t statusSeconds = 60; // report tasks running every 60 seconds.
  ACE_Time_Value statusTime = getTimeLimit( statusSeconds );
  do {
    if ( checkTimeLimit( wait ) ) {
      FWKSEVERE( "Exceeded time allowed for task group." );
      g_failCount += stopBusyClients( clients );
      failTest = true;
      remainingRuns = 0;
      running = 0;
      continue;
    }
    // start the tasks
    if ( remainingRuns > 0 ) {
      remainingRuns = startTasks( group, clients, &running, errs );
      if ( checkTimeLimit( statusTime ) ) {
        FWKINFO( "Tasks started, running: " << running << " remaining runs: " << remainingRuns );
        statusTime = getTimeLimit( statusSeconds );
      }
      if ( remainingRuns < 0 )
        failTest = true;
    }
    // poll clients for completion
    if ( running > 0 ) {
      running -= pollClients( clients );
    }
  } while ( ( remainingRuns > 0 ) || ( running > 0 ) );
  group.clear();
  clearClients( clients );
  return failTest;
}

void runTasks( FwkTest * test, TaskClientVector * clients,
  ACE_Time_Value& totalWait ) {
  bool failTest = false;
  // Set the limit on execution time for this test, if one was defined
  ACE_Time_Value testWait =  getTimeLimit( test->getWaitTime() );
  if ( testWait > totalWait )
    testWait = totalWait;

  // clear clients
  clearClients( clients );

  FwkTaskSet parGroup;
  FwkTaskSet serGroup;
  parGroup.setNoDelete();
  serGroup.setNoDelete();
  // Loop thru tasks
  FwkTask * testTask = ( FwkTask * )test->getFirst();
  while ( ( testTask != NULL ) ||
          ( serGroup.size() > 0 ) ||
          ( parGroup.size() > 0 ) ) {
    // Test our time limit for this test
    if ( checkTimeLimit( testWait ) ) {
      FWKSEVERE( "Exceeded time allowed for test " << test->getName() );
      g_failCount += stopBusyClients( clients );
      serGroup.clear();
      parGroup.clear();
      testTask = NULL;
      continue;
    }
    if ( testTask != NULL ) {
      testTask->resetTimesRan();
    }
    if ( serGroup.size() > 0 ) {
      failTest = runGroup( serGroup, clients, testWait );
    }

    if ( testTask == NULL ) {
      if ( parGroup.size() > 0 ) {
        failTest = runGroup( parGroup, clients, testWait );
      }
    }
    else {
      g_tasksRun++;
      // Assign the proper clients to the task
      assignClients( testTask, clients );
      if ( testTask->isParallel() ) {
        parGroup.add( testTask );
      }
      else {
        if ( parGroup.size() > 0 ) {
          failTest = runGroup( parGroup, clients, testWait );
        }
        serGroup.add( testTask );
      }
    }

    // next task
    if ( failTest ) {
      testTask = NULL;
      serGroup.clear();
      parGroup.clear();
    }
    else {
      if ( testTask != NULL ) {
        testTask = ( FwkTask * )test->getNext( testTask );
      }
    }
  } // end of Task while

  FWKINFO( "Done running tasks for test " << test->getName() );
}

std::string getCompilerVersion() {
  char version[50];
   #ifdef __GNUC__
     ACE_OS::sprintf(version, "gcc %d.%d.%d", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
   #else
     #ifdef __SUNPRO_CC
       ACE_OS::sprintf(version, "SunPro %x", __SUNPRO_CC);
     #else 
        #ifdef _MSC_FULL_VER
          ACE_OS::sprintf(version, "VisualStudio %d", _MSC_FULL_VER);
        #else
          #error Undefined compiler
        #endif
     #endif
   #endif
   return std::string(version);
}

void recodeDataForLatestProp(char * filename, FwkTest * test)
{
  FILE * pfil = ACE_OS::fopen( filename, "a+" );
  if ( pfil == ( FILE * )0 ) {
    FWKSEVERE( "Could not openfile" );
    exit( -1 );
  }
  char * confFile = getenv( "latestPropFile" );
  ACE_OS::fprintf(pfil,"TestName=%s\n", confFile);
  ACE_OS::fprintf( pfil, "nativeClient.version=%s %s\n", CacheFactory::getProductDescription(), getCompilerVersion().c_str() );
  ACE_OS::fprintf(pfil,"nativeClient.revision=%s\n", GEMFIRE_SOURCE_REVISION );
  ACE_OS::fprintf(pfil,"nativeClient.repository=%s\n", GEMFIRE_SOURCE_REPOSITORY );
  ACE_OS::fprintf(pfil,"hydra.Prms-testDescription=%s\n", test->getDescription() );
  ACE_OS::fflush( pfil ); 
  ACE_OS::fclose( pfil ); 
}


void runTests( TestDriver& coll, TaskClientVector * clients ) {
  // Loop thru tests
  int32_t maxTestTime = coll.getTimeValue( "maxTestTime" );
  TimeBomb tb;
  if ( maxTestTime > 0 ) {
    tb.arm( maxTestTime + 120, 0, "Exceeded maximum time allowed for a test." );
  }

  ACE_Time_Value totalWait = getTimeLimit( maxTestTime );

  // Loop thru tests
  FwkTest * test = ( FwkTest * )coll.firstTest();
  while ( test != NULL ) {
    // Test our overall time limit
    if ( checkTimeLimit( totalWait ) ) {
      FWKSEVERE( "Exceeded total time allowed for all tests." );
      g_failCount += stopBusyClients( clients );
      test = NULL;
      continue;
    }

//sb:    g_testsRun++;
    FWKINFO( "Starting test: " << test->getName() <<
      " Description: " << test->getDescription() << ". Test has " <<
      test->getCount() << " tasks. Test run will be limited to: " <<
      getTimeString( test->getWaitTime() ) );

    recodeDataForLatestProp("latest.prop",test);
    for( uint32_t i = 1; i <= test->getTimesToRun(); i++) {
      FWKINFO( "Running test: " << test->getName() << " iteration " <<
        i << " of " << test->getTimesToRun() << " times ");

    g_testsRun++;
      runTasks( test, clients, totalWait );
      // Test our overall time limit
      if ( checkTimeLimit( totalWait ) ) {
        FWKSEVERE( "Exceeded total time allowed for all tests." );
        g_failCount += stopBusyClients( clients );
        test = NULL;
        continue;
      }
    }

    test = ( FwkTest * )coll.nextTest( test );
  }

  FWKINFO( "Done running tests." );
}


/**
* DESCRIPTION:
*
*   Main function for C++ test driver program.
*/

int main( int32_t argc, char * argv[] )
{
  /* Process Command Line args */
  if ( argc < 5 ) {
    FWKSEVERE( "Usage " << argv[0] << " <log directory> <testFile> <port> " );
    return 88 ;
  }
//


//  MemRegisterTask();

  char * flocal = getenv( "GF_FQDN" );
  if ( flocal == NULL ) {
    FWKSEVERE( "flocal is not set in the environment, exiting." );
    exit( -1 );
  }
  int32_t tlimit = atoi( ACE_TEXT_ALWAYS_CHAR( argv[1] ) ); // Time in seconds we are allowed to run, 0 means forever
  const char * logDirectory = ACE_TEXT_ALWAYS_CHAR( argv[2] );
  const char * testFile = ACE_TEXT_ALWAYS_CHAR( argv[3] ); // XML file containing all required test info
  int32_t port = atoi( ACE_TEXT_ALWAYS_CHAR( argv[4] ) ); // Port number driver will accept connections on
  FwkStrCvt dport( port );
  FwkStrCvt bbPort( port + 4 );

  // Record pid
  int32_t pid = ACE_OS::getpid();
  char buf[1024];
  sprintf( buf, "../pids/%s/%s/pid_Driver", flocal, logDirectory );
  FILE * pfil = fopen( buf, "wb" );
  if ( pfil == ( FILE * )0 ) {
    FWKSEVERE( "Could not record pid, exiting." );
    exit( -1 );
  }
  fprintf( pfil, "%d\n", pid );
  fclose( pfil );

  TimeSync * ts = new TimeSync( port - 4 );

  if ( tlimit < 0 ) {
    FWKSEVERE( "No time allowed for running this test. " << tlimit );
    return 88;
  }

  TimeBomb tb;
  if ( tlimit > 0 ) {
    FWKINFO( tlimit << " seconds allowed for test." );
    tb.arm( tlimit, 88, "Exceeded maximum time allowed for all tests." );
  }

  // Need an acceptor for clients to connect to.
  const std::string dportStr = dport.toString();
  ACE_INET_Addr addr(dportStr.c_str());
  ACE_SOCK_Acceptor listener( addr, 1 );

  // Server for the BlackBoards
  uint32_t prt = bbPort.toUInt32();
//  FwkBBServer * bbServer = new FwkBBServer();
//  bbServer->start( prt );
  g_bbServer = new FwkBBServer();
  UDPMessageQueues * shared = new UDPMessageQueues( "BBQueues" );
  Receiver recv( shared, prt );
  BBProcessor serv( shared, g_bbServer );
  Responder resp( shared, prt );
  MigrateVMs *migrate = NULL;

  Service farm( 11 );
  uint32_t thrds = farm.runThreaded( &recv, 5 );
  thrds = farm.runThreaded( &serv, 1 );
  thrds = farm.runThreaded( &resp, 4 );

  char *isvmotion = getenv( "isvmotion");
  if(strcmp(isvmotion,"true")==0)
  {
    char *username = getenv("username");
    char *passwd = getenv("passwrd");
    char *vmhosts = getenv("vmhosts");
    char *vmnames = getenv("vmnames");
    char *vmInterval = getenv("vminterval");

    FWKINFO( "vmotion parameters in Driver.cpp:  username ="<< username << " passwd= " << passwd << " vmhosts=" << vmhosts << " vmnames=" << vmnames << "vmInterval="<<vmInterval); 
    //int32_t tlimit = atoi( ACE_TEXT_ALWAYS_CHAR( vmInterval) );
    migrate= new MigrateVMs(shared, username,passwd,vmhosts,vmnames,vmInterval);
    thrds = farm.runThreaded( migrate, 1 );
  }
  // load the test definition xml
  TestDriver coll( testFile );

  TaskClientVector * clients = NULL;
  bool haveClients = false;
  try {
    // Start the needed clients
    clients =
      startClients( coll, port, logDirectory, testFile, &listener );
    haveClients = true;
  } catch ( FwkException& ex ) {
    FWKSEVERE( "Caught exception starting clients: " << ex.getMessage() );
  }

  if ( haveClients ) {
    runTests( coll, clients );
  }
  else {
    FWKSEVERE( "Unable to start all clients necessary, unable to run test." );
  }

  if ( ( clients != NULL ) && ( clients->size() > 0 ) ) {
    try {
      stopClients( clients );
    } catch( FwkException& fe ) {
      FWKSEVERE( "During stopClients, encountered FwkException: " << fe.getMessage() );
    } catch( Exception& e ) {
      FWKSEVERE( "During stopClients, encountered Exception: " << e.getMessage() );
    } catch( std::exception se ) {
      FWKSEVERE( "During stopClients, encountered std::exception: " << se.what() );
    } catch( ... ) {
      FWKSEVERE( "During stopClients, encountered unknown exception." );
    }
  }

  std::string result;
  g_bbServer->dump( result );
  FWKINFO("Dump Blackboard " << result);
  farm.stopThreads();
  delete g_bbServer;
  g_bbServer = NULL;

  if ( clients != NULL ) {
    FWKINFO( "SUMMARY: Ran " << g_testsRun << " tests and " << g_tasksRun <<
      " tasks on " << clients->size() << " clients." );

    while ( clients->size() > 0 ) {
      TaskClient * clnt = clients->back();
      clients->pop_back();
      if ( clnt != NULL ) {
        try {
          delete clnt;
        } catch( FwkException & ) {}
      }
    }
  }
  else {
    FWKINFO( "SUMMARY: Failed to start clients." );
  }

  FWKINFO( "SUMMARY: " << g_runCount << " total task runs." );
  FWKINFO( "SUMMARY: " << g_passCount << " passed." );
  FWKINFO( "SUMMARY: " << g_failCount << " failed." );
  FWKINFO( "SUMMARY: " << g_clientNoExit << " clients failed to exit normally." );
  FWKINFO( "Driver complete." );
  if( g_failCount > 0 ){
    FILE * pfil = fopen("failure.txt", "wb" );
    fclose( pfil );
  }

  ts->stop();
  delete ts;
  if(migrate != NULL)
    delete migrate;
  return 0;
}  /* main() */

