/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_FAIRQUEUE2_HPP_
#define _GEMFIRE_FAIRQUEUE2_HPP_

#ifndef _SOLARIS

#include "SpinLock.hpp"
#include <deque>
#include <ace/ACE.h>
#include <ace/Thread_Semaphore.h>
#include <ace/Token.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>

namespace gemfire
{
  /** This class implements a queue that implements a producer-consumer scenario
   * with multiple producers and consumers where the producers and consumers
   * are allowed access in FIFO order.
   */
  template < class T > class FairQueue
  {
    public:

      FairQueue( ) : m_nonEmptySema( 0 ), m_closed( false )
      {
      }

      ~FairQueue( )
      {
        ACE_Guard< ACE_Token > _guardGet( m_queueGetLock );
        close( );
      }

      /**
       * Wait for for "sec" seconds until notified. If the threads have to wait
       * they do so in a FIFO order.
       */
      T* getUntil( long sec )
      {
        bool isClosed;
        T* mp = getNoGetLock( isClosed );

        if ( mp == NULL && !isClosed ) {

          ACE_Guard< ACE_Token > _guard( m_queueGetLock );

          ACE_Time_Value endTime;
          ACE_Time_Value stopAt;

          isClosed = getClosed( );
          if ( !isClosed ) {
            endTime = ACE_OS::gettimeofday( );
            endTime += sec;
            do {
              // Make a copy since sema.acquire(.) will modify the arg
              stopAt = endTime;
              m_nonEmptySema.acquire( stopAt );
              mp = getNoGetLock( isClosed );
            } while ( mp == NULL && !isClosed && stopAt < endTime );
          }
        }
        return mp;
      }

      void put( T* mp )
      {
        GF_DEV_ASSERT( mp != NULL );

        SpinLockGuard _guard( m_queueLock );
        if ( !m_closed ) {
          if ( m_queue.size( ) == 0 ) {
            m_nonEmptySema.release( );
          }
          m_queue.push_front( mp );
        }
      }

      uint32_t size( )
      {
        SpinLockGuard _guard( m_queueLock );

        return static_cast<uint32_t> (m_queue.size( ));
      }

      bool empty( )
      {
        return ( size( ) == 0 );
      }

      void reset( )
      {
        SpinLockGuard _guard( m_queueLock );

        m_closed = false;
        while ( m_nonEmptySema.tryacquire( ) != -1 );
      }

      void close( )
      {
        std::deque< T* > tmpQueue;
        {
          SpinLockGuard _guard( m_queueLock );

          m_closed = true;
          while ( !m_queue.empty() ) {
            T* mp = m_queue.back( );
            m_queue.pop_back( );
            tmpQueue.push_front( mp );
          }
          m_nonEmptySema.release( );
        }
        while ( tmpQueue.size( ) > 0 ) {
          T* mp = tmpQueue.back( );
          tmpQueue.pop_back( );
          delete mp;
        }
      }

      bool getClosed( )
      {
        SpinLockGuard _guard( m_queueLock );

        return m_closed;
      }

    private:

      inline T* getNoGetLock( bool& isClosed )
      {
        T* mp = NULL;
        SpinLockGuard _guard( m_queueLock );

        isClosed = m_closed;
        int32_t queueSize;
        if ( !isClosed && ( queueSize = (int32_t)m_queue.size( ) ) > 0 ) {
          mp = m_queue.back( );
          m_queue.pop_back( );
          if ( queueSize == 1 ) {
            // Reset the semaphore to 0 if it is 1.
            m_nonEmptySema.tryacquire( );
          }
        }
        return mp;
      }

      std::deque < T* > m_queue;
      SpinLock m_queueLock;
      ACE_Token m_queueGetLock;
      ACE_Thread_Semaphore m_nonEmptySema;
      bool m_closed;
  };

} // end namespace


#endif // _SOLARIS

#endif // ifndef _GEMFIRE_FAIRQUEUE2_HPP_
