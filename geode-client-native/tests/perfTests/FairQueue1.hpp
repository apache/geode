/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_FAIRQUEUE1_HPP_
#define _GEMFIRE_FAIRQUEUE1_HPP_

#ifndef _SOLARIS

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

      FairQueue( ) : m_nonEmptySema( 0 ), m_closed( false ), m_maxWaiters( 0 )
      {
      }

      ~FairQueue( )
      {
        ACE_Guard< ACE_Token > _guardGet( m_queueGetLock );
        close( );
      }

      /**
       * Wait for for "sec" seconds until notified. This processes requests
       * in a FIFO order.
       */
      T* getUntil( long sec )
      {
        if ( m_closed ) return 0;

        ACE_Guard< ACE_Token > _guard( m_queueGetLock );

        if ( m_closed ) return 0;

        T* mp = getNoGetLock( );

        if ( mp == 0 ) {
          ACE_Time_Value stopAt( ACE_OS::gettimeofday( ) );
          stopAt += sec;

          int waiters = m_queueGetLock.waiters( );
          if ( waiters > m_maxWaiters ) {
            m_maxWaiters = waiters;
          }
          m_nonEmptySema.acquire( stopAt );
          if ( !m_closed ) {
            ACE_Read_Guard< ACE_Token > _guard( m_queueLock );
            if ( !m_closed && m_queue.size( ) > 0 ) {
              mp = m_queue.back( );
              m_queue.pop_back( );
            }
          }
        }
        return mp;
      }

      void put( T* mp )
      {
        GF_DEV_ASSERT( mp != 0 );

        ACE_Write_Guard< ACE_Token > _guard( m_queueLock );
        if ( !m_closed ) {
          m_queue.push_front( mp );
          if ( m_queue.size( ) == 1 ) {
            m_nonEmptySema.release( );
          }
        }
      }

      uint32_t size( )
      {
        ACE_Read_Guard< ACE_Token > _guard( m_queueLock );

        return static_cast<uint32_t> (m_queue.size( ));
      }

      bool empty( )
      {
        return size( ) == 0;
      }

      void reset( )
      {
        ACE_Write_Guard< ACE_Token > _guard( m_queueLock );

        m_closed = false;
        while ( m_nonEmptySema.tryacquire( ) != -1 );
      }

      void close( )
      {
        ACE_Write_Guard< ACE_Token > _guard( m_queueLock );

        m_closed = true;
        while ( !m_queue.empty() ) {
          T* mp = m_queue.back( );
          m_queue.pop_back( );
          delete mp;
        }
        m_nonEmptySema.release( );
      }

      int maxWaiters( )
      {
        return m_maxWaiters;
      }


    private:

      T* getNoGetLock( )
      {
        T* mp = 0;
        ACE_Read_Guard< ACE_Token > _guard( m_queueLock );

        int32_t queueSize;
        if ( !m_closed && ( queueSize = (int32_t)m_queue.size( ) ) > 0 ) {
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
      ACE_Token m_queueLock;
      ACE_Token m_queueGetLock;
      ACE_Thread_Semaphore m_nonEmptySema;
      volatile bool m_closed;
      volatile int m_maxWaiters;
  };

} // end namespace


#endif // _SOLARIS

#endif // ifndef _GEMFIRE_FAIRQUEUE1_HPP_
