/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.blocks.ConnectionTable;
import com.gemstone.org.jgroups.blocks.ConnectionTableNIO;

import java.net.InetAddress;
import java.util.Properties;

public class TCP_NIO extends TCP
  {

   /*
   * (non-Javadoc)
   *
   * @see org.jgroups.protocols.TCP#getConnectionTable(long, long)
   */
  @Override // GemStoneAddition
   protected ConnectionTable getConnectionTable(long ri, long cet,
                                                InetAddress b_addr, InetAddress bc_addr, int s_port, int e_port) throws Exception {
      ConnectionTableNIO ct = null;
      if (ri == 0 && cet == 0) {
         ct = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port );
      } else {
         if (ri == 0) {
            ri = 5000;
            if(warn) log.warn("reaper_interval was 0, set it to "
                  + ri);
         }
         if (cet == 0) {
            cet = 1000 * 60 * 5;
            if(warn) log.warn("conn_expire_time was 0, set it to "
                  + cet);
         }
         ct = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port, ri, cet);
      }
      return ct;
   }

  @Override // GemStoneAddition
   public String getName() {
        return "TCP_NIO";
    }

   public int getReaderThreads() { return m_reader_threads; }
   public int getWriterThreads() { return m_writer_threads; }
   public int getProcessorThreads() { return m_processor_threads; }
   public int getProcessorMinThreads() { return m_processor_minThreads;}
   public int getProcessorMaxThreads() { return m_processor_maxThreads;}
   public int getProcessorQueueSize() { return m_processor_queueSize; }
   public int getProcessorKeepAliveTime() { return m_processor_keepAliveTime; }

   /** Setup the Protocol instance acording to the configuration string */
   @Override // GemStoneAddition
   public boolean setProperties(Properties props) {
       String str;

       str=props.getProperty("reader_threads");
       if(str != null) {
          m_reader_threads=Integer.parseInt(str);
          props.remove("reader_threads");
       }

       str=props.getProperty("writer_threads");
       if(str != null) {
          m_writer_threads=Integer.parseInt(str);
          props.remove("writer_threads");
       }

       str=props.getProperty("processor_threads");
       if(str != null) {
          m_processor_threads=Integer.parseInt(str);
          props.remove("processor_threads");
       }

      str=props.getProperty("processor_minThreads");
      if(str != null) {
         m_processor_minThreads=Integer.parseInt(str);
         props.remove("processor_minThreads");
      }

      str=props.getProperty("processor_maxThreads");
      if(str != null) {
         m_processor_maxThreads =Integer.parseInt(str);
         props.remove("processor_maxThreads");
      }

      str=props.getProperty("processor_queueSize");
      if(str != null) {
         m_processor_queueSize=Integer.parseInt(str);
         props.remove("processor_queueSize");
      }

      str=props.getProperty("processor_keepAliveTime");
      if(str != null) {
         m_processor_keepAliveTime=Integer.parseInt(str);
         props.remove("processor_keepAliveTime");
      }

      return super.setProperties(props);
   }

   private int m_reader_threads = 8;

   private int m_writer_threads = 8;

   private int m_processor_threads = 10;                    // PooledExecutor.createThreads()
   private int m_processor_minThreads = 10;                 // PooledExecutor.setMinimumPoolSize()
   private int m_processor_maxThreads = 10;                 // PooledExecutor.setMaxThreads()
   private int m_processor_queueSize=100;                   // Number of queued requests that can be pending waiting
                                                            // for a background thread to run the request.
   private int m_processor_keepAliveTime = -1;              // PooledExecutor.setKeepAliveTime( milliseconds);
                                                            // A negative value means to wait forever

}
