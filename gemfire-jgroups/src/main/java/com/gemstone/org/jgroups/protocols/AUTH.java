package com.gemstone.org.jgroups.protocols;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.protocols.pbcast.JoinRsp;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.GFLogWriter;

/**
 * 
 * AUTH is used to provide a layer of authentication to JGroups, to prevent
 * random members from joining a group. Members have to pass authentication to
 * join a group, otherwise they will be rejected.
 * 
 * This allows you to define pluggable security that defines if a node should be
 * allowed to join a group. AUTH sits below the GMS protocol and listens for
 * JOIN REQUEST messages. When a JOIN REQUEST is received it tries to find an
 * <code>AuthHeader</code> object, to obtain credentials of a joiner VM.
 * 
 * When authentication is successful, the message is simply passed up the stack
 * to the GMS protocol. When it fails, the AUTH protocol creates a JOIN RESPONSE
 * message with a failure string and passes it back down the stack. This failure
 * string informs the client of the reason for failure. Clients will then fail
 * to join the group and will throw a
 * <code> AuthenticationFailedException</code>. If this error string is null
 * then authentication is considered to have passed.
 * 
 * This allows pluggable authentication mechanisms, abstracted from the core of
 * JGroups, to be configured to secure and lock down who can join a group.
 * 
 * 
 * @author Yogesh Mahajan
 * @since 5.5
 */
public class AUTH extends Protocol {

  /** Name of the protocol */
  public static final String name = "AUTH";

  /** Flag to indicate a join request */
  private static final int JOIN_REQ_MSG = 1;

  /** Flag to indicate a new VIEW message */
  private static final int VIEW_MSG = 2;

  /** Flag to indicate a VIEW_SYNC message */
  private static final int VIEW_SYNC_MSG = 3;

  /** Flag to indicate a VIEW_CHANGE event */
  private static final int VIEW_CHANGE_EVT = 4;

  /** Flag to indicate a SET_LOCAL_ADDRESS event */
  private static final int SET_LOCAL_ADDRESS_EVT = 5;

  /** Address of this host */
  Address local_addr = null;
  
  /** guards view processing */
  private final Object viewLock = new Object();
  
  /** current view ID, guarded by viewLock */
  private ViewId vid;

  /**
   * Checks the auth header. When the header authenticated successfully, it just
   * passes up the join request and pbcast.GMS will accept the join and sends
   * the header GMS.GmsHeader.JOIN_RSP containing a pbcast.JoinRsp back.
   * 
   * When the authentication is not successful the AUTH protocol answers the
   * GMS.GmsHeader.JOIN_REQ itself and sends back a GMS.GmsHeader.JOIN_RSP
   * containing a pbcast.JoinRsp object with an error message.
   */
  @Override
  public void up(Event evt) {

    String authenticator = System
        .getProperty("gemfire.sys." // DistributionConfigImpl.SECURITY_SYSTEM_PREFIX
            +  "security-peer-authenticator"); //DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME);
    boolean isSecure = (authenticator != null && authenticator.length() != 0);
    if (isSecure) {
      String failMsg = null;
      int hdrType = getHeaderType(evt);
      String msgType = null;
      Message msg = null;
      AuthHeader authHeader = null;
      switch (hdrType) {
        case JOIN_REQ_MSG:
          msgType = "JOIN_REQ";
          msg = (Message)evt.getArg();
          authHeader = (AuthHeader)msg.getHeader(AUTH.name);
          failMsg = verifyCredentials(authenticator,
              authHeader != null ? authHeader.getCredentials() : null, msg
                  .getSrc(), msgType + " message");
          break;
          
        case VIEW_MSG:
          msgType = "VIEW";
          msg = (Message)evt.getArg();
          // ignore the view if its got an old lamport time
          View gview = msg.getObject();
          synchronized(viewLock) {
            if (vid != null && vid.compareTo(gview.getVid()) >= 0) {
              break;
            }
          }
          authHeader = (AuthHeader)msg.getHeader(AUTH.name);
          failMsg = verifyCredentials(authenticator,
              authHeader != null ? authHeader.getCredentials() : null, msg
                  .getSrc(), msgType + " message");
          break;
          
        case VIEW_SYNC_MSG:
          msgType = "VIEW_SYNC";
          msg = (Message)evt.getArg();
          View v = msg.getObject();
          // ignore the view if its got an old lamport time
          synchronized(viewLock) {
            if (vid != null && vid.compare(v.getVid()) >= 0) {
              break;
            }
          }
          authHeader = (AuthHeader)msg.getHeader(AUTH.name);
          failMsg = verifyCredentials(authenticator,
              authHeader != null ? authHeader.getCredentials() : null, msg
                  .getSrc(), msgType + " message");
          break;

        case VIEW_CHANGE_EVT:
          synchronized(viewLock) {
            View newView = (View)evt.getArg();
            vid = newView.getVid(); // save for later use
            Properties credentials = (Properties)newView.getAdditionalData();
            if (credentials == null) {
              failMsg = ExternalStrings
                        .AUTH_MISSING_CREDENTIALS_IN_MEMBERSHIP_VIEW_CHANGE
                        .toLocalizedString();
            }
            else {
              failMsg = verifyCredentials(authenticator, credentials, newView
                  .getCreator(), "VIEW_CHANGE event");
            }
          }
          break;

        case SET_LOCAL_ADDRESS_EVT:
          this.local_addr = (Address)evt.getArg();
          break;
      }
      if (failMsg == null) {
        passUp(evt);
      }
      else {
        passDown(getFailureEvent(evt, failMsg, hdrType));
      }
    }
    else { // non secure mode
      passUp(evt);
    }
  }

  /**
   * When pbcast.GMS sends a join request (a GMS.GmsHeader.JOIN_REQ header) AUTH
   * just places the credential for the authentication in its
   * <code>AuthHeader</code>. The join message will be sent to the
   * coordinator
   */
  @SuppressWarnings("fallthrough") // GemStoneAddition
  @Override
  public void down(Event evt) {

    String authInit = System
        .getProperty("gemfire.sys." // DistributionConfigImpl.SECURITY_SYSTEM_PREFIX
            +  "security-peer-auth-init"); // DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME);
    boolean isSecure = (authInit != null && authInit.length() != 0);
    if (isSecure) {
      int hdrType = getHeaderType(evt);
      GFLogWriter securityLogger = log.getSecurityLogWriter();
      Properties credentials;
      String msgType = null;
      switch (hdrType) {
        case JOIN_REQ_MSG:
//          if (msgType == null) GemStoneAddition (can only be null) 
          {
            msgType = "JOIN_REQ";
          }
          // Fall through to perform the same processing as VIEW
        // FALL THRU (GemStoneAddition)
        case VIEW_MSG:
          if (msgType == null) {
            msgType = "VIEW";
          }
          // Fall through to perform the same processing as VIEW_SYNC
        // FALL THRU (GemStoneAddition)
        case VIEW_SYNC_MSG:
          if (msgType == null) {
            msgType = "VIEW_SYNC";
          }
          // found a join or view request message - now add an AUTH Header
          Message msg = (Message)evt.getArg();
          // create a new AuthHeader to store the credentials
          AuthHeader authHeader = new AuthHeader();
          credentials = getCredentials(authInit, msg.getDest(), msgType
              + " message");
          if (credentials != null) {
            try {
              authHeader.setCredentials(credentials);
            }
            catch (IllegalArgumentException e) {
              securityLogger.severe(
                ExternalStrings.AUTH_SECURITY_CREDENTIALS_ARE_TOO_LARGE_TO_TRANSMIT_0, e);
            }
            // Add AuthHeader to the msg
            msg.putHeader(AUTH.name, authHeader);
            // successful put AuthHeader
          }
          break;

        case VIEW_CHANGE_EVT:
          synchronized(viewLock) {
            View v = (View)evt.getArg();
            vid = v.getVid(); // save the Lamport time-stamp for later use
            credentials = getCredentials(authInit, v.getVid().getCoordAddress(),
                "VIEW_CHANGE message");
            try {
              v.setAdditionalData(credentials);
            }
            catch (IllegalArgumentException e) {
              securityLogger
                  .severe(ExternalStrings
                          .AUTH_CREDENTIALS_TOO_LARGE_TO_TRANSMIT_0, 
                          e.getLocalizedMessage());
            }
          }
          break;
      }
    }
    passDown(evt);
  }

  @Override
  public final String getName() {
    return AUTH.name;
  }

  /**
   * Filter the security properties from java properties removing the leading
   * {@link DistributionConfigImpl#SECURITY_SYSTEM_PREFIX} from them.
   * 
   * @return the security properties from the java properties
   */
  private Properties getSecurityProperties() {

    Properties props = (Properties)System.getProperties().clone(); // for bug 46822
    Properties securityProps = new Properties();
    final String secPrefix =  "gemfire.sys.security-";
    Iterator propIter = props.entrySet().iterator();
    while (propIter.hasNext()) {
      Map.Entry propEntry = (Map.Entry)propIter.next();
      String propKey = (String)propEntry.getKey();
      if (propKey.startsWith(secPrefix)) {
        securityProps.setProperty(propKey
            .substring("gemfire.sys.".length()),
            (String)propEntry.getValue());
      }
    }
    return securityProps;
  }

  /**
   * Obtain the credentials using the provided AuthInitialize method.
   * 
   * @return credentials using the provided AuthInitialized method or null in
   *         case of failure
   */
  private Properties getCredentials(String authInitMethod, Address dest,
      String eventStr) {

    Properties credentials = null;
    Properties secProps = getSecurityProperties();
    GFLogWriter securityLogWriter = log.getSecurityLogWriter();
    try {
      credentials = stack.gfPeerFunctions.getCredentials(authInitMethod, secProps,
          dest, true, log.getLogWriter(),
          securityLogWriter);
    }
    catch (Exception ex) {
      securityLogWriter.warning(
        ExternalStrings.AUTH_FAILED_TO_OBTAIN_CREDENTIALS_IN_0_USING_AUTHINITIALIZE_1_2,
        new Object[] {eventStr, authInitMethod, ex.getLocalizedMessage()});
    }
    return credentials;
  }

  /**
   * Check the credentials and return a failure message if invalid.
   * 
   * @return failure message if credentials could not be verified; null if valid
   *         credentials
   */
  private String verifyCredentials(String authenticator,
      Properties credentials, Address src, String eventStr) {

    String failMsg = null;
    GFLogWriter securityLogWriter = log.getSecurityLogWriter();
    if (credentials != null) {
      // Now we have the credentials we need to validate it
      try {
          stack.gfPeerFunctions.verifyCredentials(authenticator, credentials,
              getSecurityProperties(), log.getLogWriter(), 
              securityLogWriter,
              src);

      }
      catch (Exception ex) {
        securityLogWriter.warning(
          ExternalStrings.AUTH_AUTHENTICATION_FAILED_FOR_0_FROM_1_USING_AUTHENTICATOR_2_3,
          new Object[] {eventStr, src, authenticator, ex.getLocalizedMessage()}, ex);
          failMsg = ExternalStrings.AUTH_AUTHENTICATION_FAILED_FOR_0_SEE_COORDINATOR_1_LOGS_FOR_DETAILS.toLocalizedString(new Object[] {eventStr, this.local_addr});
      }
    }
    else {
      // No credentials - need to send failure message
      securityLogWriter.warning(
        ExternalStrings.AUTH_FAILED_TO_FIND_CREDENTIALS_IN_0_FROM_1_USING_AUTHENTICATOR_2,
        new Object[] {eventStr, src, authenticator});
      failMsg = ExternalStrings.AUTH_FAILED_TO_FIND_CREDENTIALS_IN_0_SEE_COORDINATOR_1_LOGS_FOR_DETAILS.toLocalizedString(new Object[] {eventStr, this.local_addr } );
    }
    return failMsg;
  }

  /**
   * Used to return a header type for the event. Right now handles JOIN_REQ,
   * VIEW, VIEW_SYNC messages and VIEW_CHANGE, SET_LOCAL_ADDRESS events.
   * 
   * @param evt
   *                The event object passed in to AUTH
   * @return The type of event object or -1 if the event is not handled.
   */
  private int getHeaderType(Event evt) {

    switch (evt.getType()) {
      case Event.MSG:
        Message msg = (Message)evt.getArg();
        VIEW_SYNC.ViewSyncHeader vsyncHdr = (VIEW_SYNC.ViewSyncHeader)msg
            .getHeader(VIEW_SYNC.name);
        if (vsyncHdr != null
            && vsyncHdr.type == VIEW_SYNC.ViewSyncHeader.VIEW_SYNC) {
          return VIEW_SYNC_MSG;
        }
        GMS.GmsHeader gmsHdr = (GMS.GmsHeader)msg.getHeader(GMS.name);
        if (gmsHdr == null) {
          return -1;
        }
        switch (gmsHdr.getType()) {
          case GMS.GmsHeader.JOIN_REQ:
            return JOIN_REQ_MSG;
          case GMS.GmsHeader.VIEW:
            return VIEW_MSG;
          default:
            return -1;
        }

      case Event.VIEW_CHANGE:
        return VIEW_CHANGE_EVT;

      case Event.SET_LOCAL_ADDRESS:
        return SET_LOCAL_ADDRESS_EVT;
    }
    return -1;
  }

  /**
   * Used to create failed JOIN_RSP and VIEW messages to pass back down the
   * stack
   * 
   * @param evt
   *                The source event
   * @param message
   *                The failure message to send back to the joiner
   * @return An Event containing the failure message
   */
  private Event getFailureEvent(Event evt, String message, int hdrType) {

    Address src = null;
    View recvView;
    GMS.GmsHeader gmsHeader = null;
    Serializable messagePayload = null;
    GFLogWriter securityLogger = log.getSecurityLogWriter();
    switch (hdrType) {
      case JOIN_REQ_MSG:
        messagePayload = new JoinRsp(message);
        gmsHeader = new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP);
        src = ((Message)evt.getArg()).getSrc();
        break;

      case VIEW_MSG:
        // Fall through since both VIEW and VIEW_SYNC send back the same
        // VIEW_ACK message
      case VIEW_SYNC_MSG:
        Message recvMsg = (Message)evt.getArg();
        recvView = recvMsg.getObject();
        try {
          recvView.setAdditionalData(message);
        }
        catch (IllegalArgumentException e) {
          securityLogger.severe(ExternalStrings.AUTH_CREDENTIALS_TOO_LARGE_TO_TRANSMIT_0, e.getLocalizedMessage());
        }
        gmsHeader = new GMS.GmsHeader(GMS.GmsHeader.VIEW_ACK);
        messagePayload = recvView;
        src = recvMsg.getSrc();
        break;

      case VIEW_CHANGE_EVT:
        recvView = (View)evt.getArg();
        recvView.setAdditionalData(message);
        gmsHeader = new GMS.GmsHeader(GMS.GmsHeader.VIEW_ACK);
        messagePayload = recvView;
        src = recvView.getCreator();
        break;
    }
    Message msg = new Message(src, this.local_addr, null);
    msg.putHeader(GMS.name, gmsHeader);
    msg.setObject(messagePayload);
    return new Event(Event.MSG, msg);
  }

}
