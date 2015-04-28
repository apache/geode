/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
//$Id: TcpRingNode.java,v 1.4 2005/08/08 12:45:41 belaban Exp $

package com.gemstone.org.jgroups.protocols.ring;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.SuspectedException;
import com.gemstone.org.jgroups.TimeoutException;
import com.gemstone.org.jgroups.blocks.GroupRequest;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.RpcProtocol;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;


public class TcpRingNode implements RingNode
{

   final ServerSocket tokenReceiver;
   Socket previous,next;
   final Address thisNode;
     Address nextNode;
   ObjectInputStream ios;
   ObjectOutputStream oos;
   final RpcProtocol rpcProtocol;
   static/*GemStoneAddition*/ final boolean failedOnTokenLostException = false;

   final Object socketMutex = new Object();
    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());

   public TcpRingNode(RpcProtocol owner, Address memberAddress)
   {
      tokenReceiver = Util.createServerSocket(12000);
      rpcProtocol = owner;
      thisNode = memberAddress;
      nextNode = null;
   }

   public IpAddress getTokenReceiverAddress()
   {
      return new IpAddress(tokenReceiver.getLocalPort());
   }

   public Object receiveToken(int timeout) throws TokenLostException
   {
      RingToken token = null;
      Address wasNextNode = nextNode;
      try
      {
         if (previous == null)
         {
            previous = tokenReceiver.accept();
            ios = new ObjectInputStream((previous.getInputStream()));

         }
         previous.setSoTimeout(timeout);
         token = new RingToken();
         token.readExternal(ios);
      }
      catch (InterruptedIOException io)
      {
         //read was blocked for more than a timeout, assume token lost
         throw new TokenLostException(io.getMessage(), io, wasNextNode, TokenLostException.WHILE_RECEIVING);
      }
      catch (ClassNotFoundException cantHappen)
      {
      }
      catch (IOException ioe)
      {
         closeSocket(previous);
         previous = null;
         if (ios != null)
         {
            try
            {
               ios.close();
            }
            catch (IOException ignored)
            {
            }
         }

         token = (RingToken) receiveToken(timeout);
      }
      return token;
   }

   public Object receiveToken() throws TokenLostException
   {
      return receiveToken(0);
   }

   public void passToken(Object token) throws TokenLostException
   {
      synchronized (socketMutex)
      {
         try
         {
            ((Externalizable)token).writeExternal(oos);
            oos.flush();
            oos.reset();
         }
         catch (IOException e)
         {
            e.printStackTrace();
            //something went wrong with the next neighbour while it was receiving
            //token, assume token lost
            throw new TokenLostException(e.getMessage(), e, nextNode, TokenLostException.WHILE_SENDING);
         }

      }
   }

   public void tokenArrived(Object token)
   {
      //not needed , callback for udp ring
   }

   public void reconfigureAll(Vector newMembers)
   {

   }


   public void reconfigure(Vector newMembers)
   {

      if (isNextNeighbourChanged(newMembers))
      {
         IpAddress tokenReceiverAddress = null; // GemStoneModification
         synchronized (socketMutex)
         {
            nextNode = getNextNode(newMembers);

               if(log.isInfoEnabled()) log.info(ExternalStrings.TcpRingNode_NEXT_NODE__0, nextNode);

            try
            {
               tokenReceiverAddress = (IpAddress) rpcProtocol.callRemoteMethod(nextNode, "getTokenReceiverAddress", GroupRequest.GET_FIRST, 0);
            }
            catch (TimeoutException tim)
            {
               if(log.isErrorEnabled()) log.error(ExternalStrings.TcpRingNode_TIMEOUTED_WHILE_DOING_RPC_CALL_GETTOKENRECEIVERADDRESS_0, tim);
               tim.printStackTrace();
            }
            catch (SuspectedException sus)
            {
               if(log.isErrorEnabled()) log.error(ExternalStrings.TcpRingNode_SUSPECTED_NODE_WHILE_DOING_RPC_CALL_GETTOKENRECEIVERADDRESS_0, sus);
               sus.printStackTrace();
            }
            try
            {
               closeSocket(next);
               next = new Socket(tokenReceiverAddress.getIpAddress(), tokenReceiverAddress.getPort());
               next.setTcpNoDelay(true);
               oos = new ObjectOutputStream(next.getOutputStream());
            }
            catch (IOException ioe)
            {
               if(log.isErrorEnabled()) log.error(ExternalStrings.TcpRingNode_COULD_NOT_CONNECT_TO_NEXT_NODE__0, ioe);
               ioe.printStackTrace();
            }
         }
      }
   }

   private void closeSocket(Socket socket)
   {
      if (socket == null) return;
      try
      {
         socket.close();
      }
      catch (IOException ioe)
      {
         ioe.printStackTrace();
      }

   }

   private boolean isNextNeighbourChanged(Vector newMembers)
   {
      Address oldNeighbour = nextNode;
      Address newNeighbour = getNextNode(newMembers);
      return !(newNeighbour.equals(oldNeighbour));
   }

   private Address getNextNode(Vector otherNodes)
   {
      int myIndex = otherNodes.indexOf(thisNode);
      return (myIndex == otherNodes.size() - 1)?
            (Address) otherNodes.firstElement():
            (Address) otherNodes.elementAt(myIndex + 1);

   }
}
