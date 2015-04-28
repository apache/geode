/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: RpcProtocol.java,v 1.6 2005/11/03 11:42:59 belaban Exp $

package com.gemstone.org.jgroups.stack;



import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.blocks.MethodCall;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.RspList;
import com.gemstone.org.jgroups.util.Util;

import java.util.Vector;




/**
 * Base class for group RMC peer protocols.
 * @author Bela Ban
 */
public class RpcProtocol extends MessageProtocol  {


  @Override // GemStoneAddition
    public String getName() {
        return "RpcProtocol";
    }



    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     Class[] types, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }

    public RspList callRemoteMethods(Vector dests, String method_name, Object[] args,
                                     String[] signature, int mode, long timeout) {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethods(dests, method_call, mode, timeout);
    }


    public RspList callRemoteMethods(Vector dests, MethodCall method_call, int mode, long timeout) {
        byte[] buf=null;
        Message msg=null;

        try {
            buf=Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcProtocol_EXCEPTION_0, e);
            return null;
        }

        msg=new Message(null, null, buf);
        return castMessage(dests, msg, mode, timeout);
    }


    public Object callRemoteMethod(Address dest, String method_name, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        return callRemoteMethod(dest, method_name, new Object[]{}, new Class[]{}, mode, timeout);
    }


    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   Class[] types, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, args, types);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }

    public Object callRemoteMethod(Address dest, String method_name, Object[] args,
                                   String[] signature, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        MethodCall method_call=new MethodCall(method_name, args, signature);
        return callRemoteMethod(dest, method_call, mode, timeout);
    }


    public Object callRemoteMethod(Address dest, MethodCall method_call, int mode, long timeout)
            throws TimeoutException, SuspectedException {
        byte[] buf=null;
        Message msg=null;

        try {
            buf=Util.objectToByteBuffer(method_call);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcProtocol_EXCEPTION_0, e);
            return null;
        }

        msg=new Message(dest, null, buf);
        return sendMessage(msg, mode, timeout);
    }


    /**
     Message contains MethodCall. Execute it against *this* object and return result.
     Use MethodCall.invoke() to do this. Return result.
     */
    @Override // GemStoneAddition
    public Object handle(Message req) {
        Object     body=null;
        MethodCall method_call;

        if(req == null || req.getLength() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcProtocol_MESSAGE_OR_MESSAGE_BUFFER_IS_NULL);
            return null;
        }

        try {
            body=req.getObject();
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcProtocol_EXCEPTION_0, e);
            return e;
        }

        if(body == null || !(body instanceof MethodCall)) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcProtocol_MESSAGE_DOES_NOT_CONTAIN_A_METHODCALL_OBJECT);
            return null;
        }

        method_call=(MethodCall)body;
        try {
            return method_call.invoke(this);
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable x) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RpcProtocol_FAILED_INVOKING_METHOD__0, method_call.getName(), x);
            return x;
        }
    }


    /**
     Handle up event. Return false if it should not be passed up the stack.
     */
    @Override // GemStoneAddition
    public boolean handleUpEvent(Event evt) {
        return true;
    }


    /**
     Handle down event. Return false if it should not be passed down the stack.
     */
    @Override // GemStoneAddition
    public boolean handleDownEvent(Event evt) {
        return true;
    }


}
