/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#include <gfcpp/GemfireCppCache.hpp>

#ifndef WIN32
#include <sched.h>
#endif

#include <ace/Task.h>
#include <gfcpp/EntryEvent.hpp>
#include <gfcpp/RegionEvent.hpp>
// #define _EVENT_LOG

using namespace gemfire;

// #define _NO_QUEUE

#include <deque>

class QueuedEventTask
{
  public:
    QueuedEventTask( const CacheableKeyPtr& key, const CacheablePtr& value );
    virtual ~QueuedEventTask( );

    virtual void process( ) = 0;

  protected:
    CacheableKeyPtr m_key;
    CacheablePtr m_value;
};

class EventTaskQueue
: virtual public ACE_Task_Base
{
  public:
    EventTaskQueue( );
    virtual ~EventTaskQueue( );
    QueuedEventTask* get( );
    //Unhide function to prevent SunPro Warnings
    using ACE_Task_Base::put;
    void put( QueuedEventTask* );

    //--- override task svc method.
    virtual int svc( );

  private:
    std::deque< QueuedEventTask* > m_queue;
    bool m_shutdown;
    ACE_Thread_Mutex m_qlock;

};


QueuedEventTask::QueuedEventTask( const CacheableKeyPtr& key, const CacheablePtr& value )
: m_key( key ),
  m_value( value )
{
}


QueuedEventTask::~QueuedEventTask( )
{
  m_key = NULL;
  m_value = NULL;
}



EventTaskQueue::EventTaskQueue( )
: ACE_Task_Base( ),
  m_queue( ),
  m_shutdown( false ),
  m_qlock( )
{
#if !defined( _NO_QUEUE )
  activate();
#endif
}

EventTaskQueue::~EventTaskQueue( )
{
  m_shutdown = true;
  wait();
}

QueuedEventTask* EventTaskQueue::get( )
{
  ACE_Guard< ACE_Thread_Mutex > _guard( m_qlock );
  QueuedEventTask* event = NULL;
  if ( m_queue.size() > 0 ) {
    event = m_queue.back();
    m_queue.pop_back();
  }
  return event;
}

void EventTaskQueue::put( QueuedEventTask* event )
{
#ifdef _NO_QUEUE
  try {
    event->process();
  } catch ( ... ) {
    fprintf( stdout, "########## Exception processing non-queued event ##########\n" );
  fflush( stdout );
    traderFailed = true;
  }
#else
  ACE_Guard< ACE_Thread_Mutex > _guard( m_qlock );
  m_queue.push_front( event );
#endif
}

/** get an item from the queue and process it. */
int EventTaskQueue::svc( )
{
  QueuedEventTask* work = NULL;
  bool sleepTime = false;
  while( ! m_shutdown ) {
    work = get();
    if ( work != NULL ) {
      sleepTime = false;
      work->process();
      delete work;
      work = NULL;
    } else {
      // yield here? don't want to be a hog, or too submissive..
      if ( sleepTime ) {
        ACE_OS::thr_yield();
      } else {
        sleepTime = true;
      }
    }
  }
  return 0;
}

class Forwarder : public CacheListener {
  RegionPtr dest;
  EventTaskQueue queue;
  bool m_isAck;


public:
  Forwarder(RegionPtr& reg) : CacheListener(), dest(reg), queue()
        {
#ifdef _EVENT_LOG
    char buf[1000];
    sprintf( buf, "Created forwarder to region %s", reg->getName().asChar() );
    LOG( buf );
#endif
    m_isAck = ( dest->getAttributes()->getScope() == ScopeType::DISTRIBUTED_ACK );
  }

  void createOrUpdate( const CacheableKeyPtr& key, const CacheablePtr& newValue );

  virtual void afterCreate( const EntryEvent& event );

  virtual void afterUpdate( const EntryEvent& event );
};

class ForwardingTask : public QueuedEventTask
{
    Forwarder* m_forwarder;

  public:
    ForwardingTask( Forwarder* forwarder, const CacheableKeyPtr& key, const CacheablePtr& value )
    : QueuedEventTask( key, value ), m_forwarder( forwarder )
    {
    }

    virtual void process( )
    {
      m_forwarder->createOrUpdate( m_key, m_value );
    }

    virtual ~ForwardingTask()
    {
      m_forwarder = NULL;
    }
};

void Forwarder::createOrUpdate(const CacheableKeyPtr& key, const CacheablePtr& newValue)
{
#ifdef _EVENT_LOG
  LOG( "forwarding." );
#endif
      try {
        if (dest == NULL) {
          LOG( "IllegalState, dest == NULL" );
          return;
        }
  if ( newValue == NULL ) {
    fprintf( stdout, "event called with null value... unexpected..\n" );
    fflush( stdout );
  }
  CacheableStringPtr tmp = dynamic_cast<CacheableString *>( &(*newValue) );
    dest->put(key, newValue);
      } catch ( ... ) {
        char keyText[100];
        key->logString( keyText, 100 );
  fprintf( stdout, "Exception while forwarding key %s for region %s\n", keyText, dest->getName() );
  fflush( stdout );
  traderFailed = true;
      }
}

void Forwarder::afterCreate( const EntryEvent& event )
{
  /** It is GemStone's recommendation that CacheListener's refrain from
   * costly operations. With that said, queueing the application's reaction
   * to these events is beyond the scope of this example.
   */

#ifdef _EVENT_LOG
  LOG( "received something." );
#endif
  ForwardingTask* ft_ptr = new ForwardingTask( this, event.getKey(), event.getNewValue() );
  queue.put( ft_ptr );
}

void Forwarder::afterUpdate( const EntryEvent& event )
{
  /** It is GemStone's recommendation that CacheListener's refrain from
   * costly operations. With that said, queueing the application's reaction
   * to these events is beyond the scope of this example.
   */

#ifdef _EVENT_LOG
  LOG( "received something." );
#endif
  ForwardingTask* ft_ptr = new ForwardingTask( this, event.getKey(), event.getNewValue() );
  queue.put( ft_ptr );
}

class ReceiveCounter : public CacheListener {
  int* m_received;
  public:
    ReceiveCounter( int* counter ) : CacheListener( ), m_received( counter )
    {
#ifdef _EVENT_LOG
    LOG( "Created ReceiveCounter" );
#endif
    }

  virtual void afterCreate( const EntryEvent& event )
  {
    (*m_received)++;
#ifdef _EVENT_LOG
    LOG( "received something." );
#endif
  }

  virtual void afterUpdate( const EntryEvent& event )
  {
    (*m_received)++;
#ifdef _EVENT_LOG
    LOG( "received something." );
#endif
  }
};

class Swapper : public CacheListener {
  RegionPtr src;
  RegionPtr dest;
  EventTaskQueue queue;
  bool m_isAck;

public:
  Swapper(RegionPtr& srcRegion, RegionPtr& destRegion) : CacheListener( ), src(srcRegion), dest(destRegion), queue()
        {
#ifdef _EVENT_LOG
    char buf[1000];
    sprintf( buf, "Created swapper from %s to region %s", srcRegion->getName().asChar(), destRegion->getName().asChar() );
    LOG( buf );
#endif
    m_isAck = ( src->getAttributes()->getScope() == ScopeType::DISTRIBUTED_ACK );
  }

  void createOrUpdate( const CacheableKeyPtr& key, const CacheablePtr& newValue);

  virtual void afterCreate( const EntryEvent& event );

  virtual void afterUpdate( const EntryEvent& event );

  inline bool isAck( )
  {
    return m_isAck;
  }
};

class SwappingTask : virtual public QueuedEventTask
{
  Swapper* m_swapper;

  public:

    SwappingTask( Swapper* swapper, const CacheableKeyPtr& key, const CacheablePtr& value )
    : QueuedEventTask( key, value ), m_swapper( swapper )
    {
    }

    virtual void process( )
    {
      m_swapper->createOrUpdate( m_key, m_value );
    }

    virtual ~SwappingTask( )
    {
      m_swapper = NULL;
    }
};

void Swapper::createOrUpdate(const CacheableKeyPtr& key, const CacheablePtr& newValue)
{
  /** It is GemStone's recommendation that CacheListener's refrain from
   * costly operations. With that said, queueing the application's reaction
   * to these events is beyond the scope of this example.
   */

#ifdef _EVENT_LOG
  LOG( "swapping something." );
#endif
  try {
    if (dest == NULL) {
      LOG( "IllegalState, dest == NULL" );
      return;
    }
  if ( newValue == NULL ) {
    fprintf( stdout, "event called with null value... unexpected..\n" );
    fflush( stdout );
  }
  CacheableStringPtr tmp = dynCast<CacheableStringPtr>( newValue );
  // supposed to get the old value from the cache, but to track latency, we need the
  // new value... so, we'll get, but then put the newValue on its way back to process A.
  CacheableStringPtr cachedValuePtr =
    dynCast<CacheableStringPtr>( src->get( key ) );
#ifdef _EVENT_LOG
  LOG( "swapper get succeeded." );
#endif
  int tries = (isAck() ? 1 : 30);
  while (tries--)
  {
    if ( cachedValuePtr == NULL ) {
      dunit::sleep(10);
      cachedValuePtr = dynCast<CacheableStringPtr>( src->get( key ) );
    }
  }
  if ( cachedValuePtr == NULL ) {
    fprintf( stdout, "Error, didn't find value in the cache.\n" );
    fflush( stdout );
  } else {
    dest->put(key, newValue);
#ifdef _EVENT_LOG
    LOG( "swapper put succeeded." );
#endif
  }
  } catch ( gemfire::Exception& ex ) {
    char keyText[100];
    key->logString( keyText, 100 );
    fprintf( stdout, "Exception occured while swapping %s for region %s.\n", keyText, dest->getName() );
    fflush( stdout );
    ex.printStackTrace();
    traderFailed = true;
  }
}

void Swapper::afterCreate( const EntryEvent& event )
{
#ifdef _EVENT_LOG
  LOG( "received something." );
#endif
  SwappingTask* swap_ptr = new SwappingTask( this, event.getKey(), event.getNewValue() );
  queue.put( swap_ptr );
}

void Swapper::afterUpdate( const EntryEvent& event )
{
#ifdef _EVENT_LOG
  LOG( "received something." );
#endif
  SwappingTask* swap_ptr = new SwappingTask( this, event.getKey(), event.getNewValue() );
  queue.put( swap_ptr );
}

