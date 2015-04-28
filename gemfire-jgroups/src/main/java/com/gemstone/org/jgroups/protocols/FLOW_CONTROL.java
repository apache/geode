/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: FLOW_CONTROL.java,v 1.10 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.blocks.GroupRequest;
import com.gemstone.org.jgroups.stack.MessageProtocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.ReusableThread;
import com.gemstone.org.jgroups.util.RspList;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;


// @todo Handle view changes (e.g., members {A,B,C}, blocked on C, and C crashes --&gt; unblock).
/**
 * FLOW_CONTROL provides end-end congestion control and flow control.
 * Attempts to maximize through put, by minimizing the
 * possible block times(Forward flow control). Initially, sender starts with a smaller
 * window size <code> W</code> and large expected RTT <code>grpRTT</code>. Sender also
 * keeps a margin in the window size. When the margin is hit, insted of waiting for the
 * window size to be exhausted, sender multicasts a FLOW_CONTROL info request message.
 * If the window size is exhausted before the responses are received, send will be blocked.
 * FCInfo(flow control info) from all the receivers is gathered at the sender, and current RTT
 * is computed. If the current RTT is greater than estimated RTT window size and margin are reduced,
 * otherwise they are increased.
 * <p>
 * Horizontal interaction is initiated by the sender with the other group members.
 * <p>
 * <em>Note: A reliable transport layer is required for this protocol to function properly.</em>
 * With little effort this can be made completely independent.
 * <p>
 * <br> Also block on down() instead of sending BLOCK_SEND.
 *
 * @author Ananda Bollu
 */

public class FLOW_CONTROL extends MessageProtocol implements Runnable {
    private int _numMSGsSentThisPeriod=0;
    private static final String FLOW_CONTROL="FLOW_CONTROL";
    private final HashMap _rcvdMSGCounter=new HashMap();

    private int _windowSize=1000;
    private int _fwdMarginSize=200;
    private int _estimatedRTT=100000;
    private boolean waitingForResponse=false;
    private final ReusableThread _reusableThread;
    private double RTT_WEIGHT=0.125;
    private int _msgsSentAfterFCreq=0;
//    private final double TIME_OUT_FACTOR=0.25;//if resp not received from more than n*TIME_OUT_INCREMENT_FACTOR
//    private final double TIME_OUT_INCR_MULT=1.25;
    private double WINDOW_SIZE_REDUCTION=0.75;
    private double WINDOW_SIZE_EXPANSION=1.25;
    private boolean isBlockState=false;

    private int _windowsize_cap=1000000; //initial window size can not be more than 10^6 messages.

    public FLOW_CONTROL() {
        _reusableThread=new ReusableThread(FLOW_CONTROL);
    }

    @Override // GemStoneAddition
    public String getName() {
        return FLOW_CONTROL;
    }

    /**
     * If Event.MSG type is received count is incremented by one,
     * and message is passed to the down_prot. At some point,
     * based on the algorithm(FLOW_CONTROL protocol definition)
     * data collection sequence is started. This is done by each
     * member in SENDER role when _numMSGsSentThisPeriod hits the margin.
     * Before rsp arrives only _fwdMarginSize number of messages can be sent,
     * and then sender will be blocked.
     */
    @Override // GemStoneAddition
    public boolean handleDownEvent(Event evt) {
        if(evt.getType() == Event.MSG) {
            _numMSGsSentThisPeriod++;
            if((_numMSGsSentThisPeriod > (_windowSize - _fwdMarginSize)) && !waitingForResponse) {
                waitingForResponse=true;
                //wait for the previous request to return.before assigning a new task.
                _reusableThread.waitUntilDone();
                _reusableThread.assignTask(this);
            }
            if(waitingForResponse) {
                _msgsSentAfterFCreq++;
                if((_msgsSentAfterFCreq >= _fwdMarginSize) && !isBlockState) {

                    if(log.isInfoEnabled()) log.info(ExternalStrings.FLOW_CONTROL_ACTION_BLOCK);
                    log.error(ExternalStrings.FLOW_CONTROL_0_0_1, new Object[] {Long.valueOf(System.currentTimeMillis()), Integer.valueOf(_windowSize)});
                    passUp(new Event(Event.BLOCK_SEND));
                    isBlockState=true;
                }
            }
        }
        return true;
    }

    /**
     * If Event.MSG type is received message, number of received
     * messages from the sender is incremented. And the message is
     * passed up the stack.
     */
    @Override // GemStoneAddition
    public boolean handleUpEvent(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            Address src=msg.getSrc();
            FCInfo fcForSrc=(FCInfo)_rcvdMSGCounter.get(src);
            if(fcForSrc == null) {
                fcForSrc=new FCInfo();
                _rcvdMSGCounter.put(src, fcForSrc);
            }
            fcForSrc.increment(1);

            if(log.isInfoEnabled()) log.info(ExternalStrings.FLOW_CONTROL_MESSAGE__0__RECEIVED_FROM__1, new Object[] {Integer.valueOf(fcForSrc.getRcvdMSGCount()), src});
        }
        return true;
    }

    /**
     * Called when a request for this protocol layer is received.
     * Processes and return value is sent back in the reply.
     * FLOW_CONTROL protocol of all members gets this message(including sender?)
     *
     * @return Object containing FC information for sender with senderID.
     *         <b>Callback</b>. Called when a request for this protocol layer is received.
     */
    @Override // GemStoneAddition
    public Object handle(Message req) {
        Address src=req.getSrc();
        Long resp=Long.valueOf(((FCInfo)_rcvdMSGCounter.get(src)).getRcvdMSGCount());

        if(log.isInfoEnabled()) log.info(ExternalStrings.FLOW_CONTROL_REQEST_CAME_FROM__0__PREPARED_RESPONSE__1, new Object[] {src, resp});
        return resp;
    }

    /**
     * FCInfo request must be submitted in a different thread.
     * handleDownEvent() can still be called to send messages
     * while waiting for FCInfo from receivers. usually takes
     * RTT.
     */
    public void run() {

        if(log.isInfoEnabled()) log.info(ExternalStrings.FLOW_CONTROL__HIT_THE__FWDMARGIN_REMAINING_SIZE__0, _fwdMarginSize);
        reqFCInfo();
    }

    /**
     * Following parameters can be optionally supplied:
     * <ul>
     * <li>window size cap - <code>int</code> Limits the window size to a reasonable value.
     * <li>window size - <code>int</code> these many number of messages are sent before a block could happen
     * <li>forward margin -<code>int</code> a request for flow control information is sent when remaining window size hits this margin
     * <li>RTT weight -<code>double</code> Max RTT in the group is calculated during each Flow control request. lower number assigns
     * higher weight to current RTT in estimating RTT.
     * <li>window size reduction factor -<code>double</code> When current RTT is greater than estimated RTT current window size
     * is reduced by this multiple.
     * <li>window size expansion factor -<code>double</code> When current RTT is less than estimated RTT window is incremented
     * by this multiple.
     * </ul>
     *
     * @see com.gemstone.org.jgroups.stack.Protocol#setProperties(Properties)
     */
    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str=null;
        String winsizekey="window_size";
        String fwdmrgnkey="fwd_mrgn";
        String rttweightkey="rttweight";
        String sizereductionkey="reduction";
        String sizeexpansionkey="expansion";
        String windowsizeCapKey="window_size_cap";

        super.setProperties(props);
        str=props.getProperty(windowsizeCapKey);
        if(str != null) {
            _windowsize_cap=Integer.parseInt(str);
            props.remove(windowsizeCapKey);
        }
        str=props.getProperty(winsizekey);
        if(str != null) {
            _windowSize=Integer.parseInt(str);
            if(_windowSize > _windowsize_cap)
                _windowSize=_windowsize_cap;
            props.remove(winsizekey);
        }

        str=props.getProperty(fwdmrgnkey);
        if(str != null) {
            _fwdMarginSize=Integer.parseInt(str);
            props.remove(fwdmrgnkey);
        }

        str=props.getProperty(rttweightkey);
        if(str != null) {
            RTT_WEIGHT=Double.parseDouble(str);
            props.remove(rttweightkey);
        }

        str=props.getProperty(sizereductionkey);
        if(str != null) {
            WINDOW_SIZE_REDUCTION=Double.parseDouble(str);
            props.remove(sizereductionkey);
        }

        str=props.getProperty(sizeexpansionkey);
        if(str != null) {
            WINDOW_SIZE_EXPANSION=Double.parseDouble(str);
            props.remove(sizeexpansionkey);
        }


        if(props.size() > 0) {
            log.error(ExternalStrings.FLOW_CONTROL_FLOW_CONTROLSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;

    }

    /*-----------private stuff ------*/

    private RspList reqFCInfo() {
        RspList rspList=null;
        long reqSentTime=0, rspRcvdTime=0;
        try {
            reqSentTime=System.currentTimeMillis();
            //alternatively use _estimatedRTT for timeout.(timeout is the right way, but need to
            //check the use cases.
            rspList=castMessage(null, new Message(null, null, Util.objectToByteBuffer(FLOW_CONTROL)),
                    GroupRequest.GET_ALL, 0);
            rspRcvdTime=System.currentTimeMillis();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }

        /*If NAKACK layer is present, if n+1 th message is FLOW_CONTROL Request, if responses are received
          that means all n messages sent earlier are received(?), ignore NAK_ACK.
        */
        //ANALYSE RESPONSES

        long currentRTT=rspRcvdTime - reqSentTime;

        if(currentRTT > _estimatedRTT) {
            _windowSize=(int)(_windowSize * WINDOW_SIZE_REDUCTION);
            _fwdMarginSize=(int)(_fwdMarginSize * WINDOW_SIZE_REDUCTION);
        }
        else {
            _windowSize=(int)(_windowSize * WINDOW_SIZE_EXPANSION);
            if(_windowSize > _windowsize_cap)
                _windowSize=_windowsize_cap;
            _fwdMarginSize=(int)(_fwdMarginSize * WINDOW_SIZE_EXPANSION);
        }

        _estimatedRTT=(int)((RTT_WEIGHT * currentRTT) + (1.0 - RTT_WEIGHT) * _estimatedRTT);

        //reset for new FLOW_CONTROL request period.
        _numMSGsSentThisPeriod=0;
        waitingForResponse=false;
        _msgsSentAfterFCreq=0;

        if(isBlockState) {

            if(warn) log.warn("ACTION UNBLOCK");
            passUp(new Event(Event.UNBLOCK_SEND));
            log.error(ExternalStrings.FLOW_CONTROL_1_0_1, new Object[] {Long.valueOf(System.currentTimeMillis()), Integer.valueOf(_windowSize)});
            isBlockState=false;
        }


        if(warn) log.warn("estimatedTimeout = " + _estimatedRTT);
        if(warn) log.warn("window size = " + _windowSize + " forward margin size = " + _fwdMarginSize);

        return rspList;
    }


    /* use this instead of Integer. */
    private static class FCInfo implements Serializable {
        int _curValue;
        private static final long serialVersionUID = -8365016426836017979L;

        FCInfo() {
        }

        public void increment(int i) {
            _curValue+=i;
        }

        public int getRcvdMSGCount() {
            return _curValue;
        }

        @Override // GemStoneAddition
        public String toString() {
            return Integer.toString(_curValue);
        }
    }


}

