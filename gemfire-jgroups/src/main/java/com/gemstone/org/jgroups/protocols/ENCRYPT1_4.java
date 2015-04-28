/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// changes made by mandar

// Added .* imports
// replacing SecretKey with SecretKey


// $Id: ENCRYPT1_4.java,v 1.8 2005/08/08 12:45:42 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.Properties;
import java.util.Vector;

/**
 * ENCRYPT1_4 layer. Encrypt and decrypt the group communication in JGroups
 */
public class ENCRYPT1_4 extends Protocol  {

public static class EncryptHeader extends com.gemstone.org.jgroups.Header  {
    int type;
    static final int ENCRYPT=0;
    static final int KEY_REQUEST=1;
    static final int SERVER_PUBKEY=2;
    static final int SECRETKEY=3;
    static final int SECRETKEY_READY=4;

    // adding key for Message object purpose
    static final String KEY="encrypt";

    public EncryptHeader(){}

    public EncryptHeader(int type) {
        this.type=type;
    }

    public void writeExternal(java.io.ObjectOutput out) throws IOException {
        out.writeInt(type);
    }

    public void readExternal(java.io.ObjectInput in) throws IOException, ClassNotFoundException {
        type=in.readInt();
    }

    @Override // GemStoneAddition
    public String toString() {
        return "[ENCTYPT: <variables> ]";
    }
}


    Address local_addr=null;
    Address keyServerAddr=null;
    boolean keyServer=false;
    String asymAlgorithm="RSA";
    String symAlgorithm="DES/ECB/PKCS5Padding";
    int asymInit=512;					// initial public/private key length
    int symInit=56;					// initial shared key length
    // for public/private Key
    KeyPair Kpair;			// to store own's public/private Key
    SecretKey desKey=null;
    final PublicKey pubKey=null;               // for server to store the temporary client public key
//    PublicKey serverPubKey=null;         // for client to store server's public Key GemStoneAddition(omitted)
    Cipher cipher;
    Cipher rsa;
    final Vector members=new Vector();
    final Vector notReady=new Vector();

    public ENCRYPT1_4() {
        //Provider prov = Security.getProvider("SUN");
        //Security.addProvider(prov);
    }


    @Override // GemStoneAddition
    public String getName() {
        return "ENCRYPT1_4";
    }


    /*
     * GetAlgorithm: Get the algorithm name from "algorithm/mode/padding"
     */
    private static String getAlgorithm(String s) {
        int index=s.indexOf("/");
        if(index == -1)
            return s;

        return s.substring(0, index);
    }


    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        // asymmetric key length
        str=props.getProperty("asymInit");
        if(str != null) {
            asymInit=Integer.parseInt(str);
            props.remove("asymInit");

		if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_ASYM_ALGO_BITS_USED_IS__0, asymInit);
        }

        // symmetric key length
        str=props.getProperty("symInit");
        if(str != null) {
            symInit=Integer.parseInt(str);
            props.remove("symInit");

		if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_SYM_ALGO_BITS_USED_IS__0, symInit);
        }

        // asymmetric algorithm name
        str=props.getProperty("asymAlgorithm");
        if(str != null) {
            asymAlgorithm=str;
            props.remove("asymAlgorithm");

		if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_ASYM_ALGO_USED_IS__0, asymAlgorithm);
        }

        // symmetric algorithm name
        str=props.getProperty("symAlgorithm");
        if(str != null) {
            symAlgorithm=str;
            props.remove("symAlgorithm");

		if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_SYM_ALGO_USED_IS__0, symAlgorithm);
        }
        if(props.size() > 0) {

		if(log.isErrorEnabled()) log.error(ExternalStrings.ENCRYPT1_4_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            return false;
        }


        return true;
    }

    @Override // GemStoneAddition
    public void init() throws Exception {
        // generate keys according to the specified algorithms
        // generate publicKey and Private Key using RSA
        KeyPairGenerator KpairGen=KeyPairGenerator.getInstance(getAlgorithm(asymAlgorithm));
        KpairGen.initialize(asymInit, new SecureRandom());
        Kpair=KpairGen.generateKeyPair();

        // generate secret key
        KeyGenerator keyGen=KeyGenerator.getInstance(getAlgorithm(symAlgorithm));
        keyGen.init(symInit);
        desKey=keyGen.generateKey();

        // initialize for rsa, cipher encryption/decryption
        rsa=Cipher.getInstance(asymAlgorithm);
        cipher=Cipher.getInstance(symAlgorithm);


	    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_BOTH_ASYM_AND_SYM_ALGO_INITIALIZED_WITH_THE_SINGLE_SHARED_KEY);
    }

    /** Just remove if you don't need to reset any state */
    public static void reset() {
    }

    @Override // GemStoneAddition
    public void up(Event evt) {
        Message msg;
        Message newMsg;
        EncryptHeader hdr;


	    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_EVENT_GOING_UP_IS__0, evt);

        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:

		    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_SET_ADDRESS_CALL);
                local_addr=(Address)evt.getArg();
                break;
            case Event.FIND_INITIAL_MBRS_OK:
                Vector member=(Vector)evt.getArg();

		    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_FIND_INIT_MEMBERS_CALL_LEFT_MEMBERS_ARE__0, member.size());

		
		// this check is required, to prevent keyServer= false when adding itself
		if (!keyServer)
		    keyServer=member.size() <= 0;
		
                if(member != null && member.size() > 0)
                    keyServerAddr=((PingRsp) member.firstElement()).coord_addr;
                else
                    keyServerAddr=local_addr;

                if(!keyServer) 
		    {
			
			desKey=null;

			    if(log.isDebugEnabled()) log.debug("This is not keyserver, deskey set to null");
			// client send clien's public key to server and request server's public key
			newMsg=new Message(keyServerAddr, local_addr, Kpair.getPublic().getEncoded());
			// making changes (MANDAR)
			newMsg.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.KEY_REQUEST));
			passDown(new Event(Event.MSG, newMsg));
		    }

		    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_DONE_PARSING_FOR_ENCRYPT_HEADERS_SENDING_UPWARDS_0, evt);
		passUp(evt);
                return;

            case Event.MSG:
                msg=(Message) evt.getArg();

		    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_THIS_IS_A_MESSAGE_FROM_PEER_NOT_CONTROL_HEADER_0, msg);

                // making changes (MANDAR)
                if(msg == null) {

			if(log.isDebugEnabled()) log.debug("Null message");
                    passUp(evt);
                    return;
                }

                // making changes (MANDAR)
                //Object obj=msg.peekHeader();
                Object obj=msg.removeHeader(EncryptHeader.KEY);

		    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_STRIPPING_THE_REQUIRED_PROTOCOL_HEADER);

                // if not encrypted message, pass up
                if(obj == null || !(obj instanceof EncryptHeader)) {

			if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_DROPPING_PACKAGE_AS_ENCRYPT1_4_PROTOCOL_IS_NOT_BEEN_RECOGNIZED_MSG_WILL_NOT_BE_PASSED_UP);

		    // BELA comment this out in case U think otherwise
                    //passUp(evt);
                    return;
                }

                // making changes (MANDAR)
                //hdr = (EncryptHeader)msg.removeHeader();
                hdr=(EncryptHeader) obj;

		    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_HEADER_RECEIVED_0_1, new Object[] {hdr, Integer.valueOf(hdr.type)});
                switch(hdr.type) {
                    // key request from client and send server's public key to client
                    case EncryptHeader.KEY_REQUEST:
                        try {

				if(log.isDebugEnabled()) log.debug("Request for key");
                            // store the this client to notReady list using client's address
                            notReady.addElement(msg.getSrc());
                            // store the client's public key for temporary
                            PublicKey tmpPubKey=generatePubKey(msg.getBuffer());

				if(log.isDebugEnabled()) log.debug("Generated requestors public key");

                            // send server's publicKey
                            newMsg=new Message(msg.getSrc(), local_addr, Kpair.getPublic().getEncoded());
                            // making changes (MANDAR)
                            newMsg.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.SERVER_PUBKEY));

				if(log.isDebugEnabled()) log.debug("Encoded servers public key using clients public key, only client can debug it using its private key and sending it back");
                            passDown(new Event(Event.MSG, newMsg));

			    // my changes (MANDAR)
			    rsa.init(Cipher.ENCRYPT_MODE, tmpPubKey);
			    byte[] encryptedKey = rsa.doFinal(desKey.getEncoded());

				if(log.isDebugEnabled()) log.debug(" Generated encoded key which only client can decode");
			    
                            // send shared DesKey to client
                            //   1. Decrypt desKey with server's own private Key
                            //   2. Encrypt decrypted desKey with client's own public Key
                            // encrypt encoded desKey using server's private key
			    /*
                            rsa.init(Cipher.ENCRYPT_MODE, Kpair.getPrivate());
                            byte[] decryptedKey=rsa.doFinal(desKey.getEncoded());

                            // encrypt decrypted key using client's public key
                            rsa.init(Cipher.ENCRYPT_MODE, pubKey);
                            byte[] encryptedKey=rsa.doFinal(decryptedKey);
			    */
                            //send encrypted deskey to client			    
                            newMsg=new Message(msg.getSrc(), local_addr, encryptedKey);
                            // making changes (MANDAR)
                            newMsg.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.SECRETKEY));

				if(log.isDebugEnabled()) log.debug(" Sending encoded key to client");
                            passDown(new Event(Event.MSG, newMsg));
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                            System.out.println(e + "0");
                        }
                        return;
                    case EncryptHeader.SECRETKEY_READY:						
                        //server get client's public key and generate the secret key
                        notReady.removeElement(msg.getSrc());

			    if(log.isDebugEnabled()) log.debug("Removed client " + msg.getSrc() + "from notready list");
                        return;
                    case EncryptHeader.SERVER_PUBKEY:
                        /*serverPubKey=GemStoneAddition*/generatePubKey(msg.getBuffer());

			    if(log.isDebugEnabled()) log.debug(" Obtained the servers public key");
                        return;

                    case EncryptHeader.SECRETKEY:
                        try {
                            // decrypt using client's private Key
                            rsa.init(Cipher.DECRYPT_MODE, Kpair.getPrivate());
			    // my changes (MANDAR)
			    byte[] encodedKey = rsa.doFinal(msg.getBuffer());


				if(log.isDebugEnabled()) log.debug("generating encoded key obtained from server-admin");
			    
			    /* Piece commented out by MANDAR 
                            byte[] decryptedKey=rsa.doFinal(msg.getBuffer());			    
                            // decrypt using server's public Key
                            rsa.init(Cipher.DECRYPT_MODE, serverPubKey);
                            byte[] encodedKey=rsa.doFinal(decryptedKey);
			    */

                            // decode secretKey
                            desKey=decodedKey(encodedKey);
                            if(desKey == null)
                                log.error(ExternalStrings.ENCRYPT1_4_OHH_OH__DES_KEY_IS_NULL);
				    

                            // send ready message (MANDAR) null -> ""
                            newMsg=new Message(msg.getSrc(), local_addr, null);
                            // making changes (MANDAR)
                            newMsg.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.SECRETKEY_READY));
                            passDown(new Event(Event.MSG, newMsg));

				if(log.isDebugEnabled()) log.debug("Got the deskey, sending down sec_Ready header");
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                            System.out.println(e + "5");
                        }
                        return;

                    default:
                        break;
                }

                if (hdr.type != 0)
                    log.error(ExternalStrings.ENCRYPT1_4_ERROR__HEADER_IS_NOT_0);

                // not have shared key yet
                // this encrypted message is of no use, drop it
                if(desKey == null) return;


		    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_STARTING_TO_DECYPHER_MESSAGES);

                // if both the shared key and incoming message are not null
                // decrypt the message
                if(msg.getBuffer() != null) {
                    try {
                        cipher.init(Cipher.DECRYPT_MODE, desKey);
                        msg.setBuffer(cipher.doFinal(msg.getBuffer()));
                    }
                    catch(Exception e) {
			e.printStackTrace();			
                    }
                }
                break;
        }

	    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_PASSING_UP_EVENT);
        passUp(evt);            // Pass up to the layer above us
    }

    @Override // GemStoneAddition
    public void down(Event evt) {
        Message msg;
        Message newMsg;
        boolean leave=false;


        if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_DOWNEVT_IS_0_1, new Object[] {evt, Integer.valueOf(evt.getType())});

        switch(evt.getType()) {
	    
        case Event.VIEW_CHANGE:

            if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_VIEW_CHANGE_CALL_NEW_MEMBER_COMING_IN);
            Vector new_members=((View)evt.getArg()).getMembers();

            // member size decreases: member leaves, need a new key
            if(members.size() > new_members.size()) leave=true;

            // copy member list
            synchronized(members) {
                members.removeAllElements();
                if(/*new_members != null && GemStoneAddition (cannot be null) */ new_members.size() > 0)
                    for(int i=0; i < new_members.size(); i++)
                        members.addElement(new_members.elementAt(i));
            }// end of sync

            // redistribute/regain the new key because old member leaves
            if(leave) {
                // get coordinator address
                Object obj=members.firstElement();

                // if I'm the coordinator/key-server
                if(obj.equals(local_addr)) {
                    //create the new shared key and distribute
                    keyServer=true;
                    keyServerAddr=local_addr;

                    // reset shared key
                    desKey=null;

                    if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_LEAVE_CAUSED_DESKEY_TO_BE_NULL);

                    try {
                        //generate new shared key
                        KeyGenerator keyGen=KeyGenerator.getInstance(getAlgorithm(symAlgorithm));
                        keyGen.init(symInit);
                        desKey=keyGen.generateKey();
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }//end of local_addr == obj
                // if I'm not the coordinator/key-server
                else {
                    keyServer=false;
                    keyServerAddr=(Address)obj;

                    // reset shared key
                    desKey=null;

                    // client send clien's public key to server and request server's public key
                    newMsg=new Message(keyServerAddr, local_addr, Kpair.getPublic().getEncoded());
                    // making changes (MANDAR)
                    newMsg.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.KEY_REQUEST));
                    passDown(new Event(Event.MSG, newMsg));

                    if(log.isDebugEnabled()) log.debug("Requesting new key to be part of group");
                } // end of else
            }
            break;

        case Event.MSG:
            msg= (Message) evt.getArg();

            if(log.isDebugEnabled()) log.debug("Its a message call " + msg);
            int i;
		
            // For Server:
            // if some members don't have the shared key yet
            if(!notReady.isEmpty())
            {
                System.out.println("not Ready list  :" + notReady.toString());
                if(msg.getDest() == null) {
                    for(i=0; i < notReady.size(); i++) {
                        // making changes (MANDAR)
                        newMsg=new Message((Address)notReady.elementAt(i), local_addr, msg.getBuffer());
                        passDown(new Event(Event.MSG, newMsg));
                    }
                    break;
                }
                else
                {
                    for(i=0; i < notReady.size(); i++) {
                        if(msg.getDest().equals(notReady.elementAt(i))) {
                            passDown(evt);
                            return;
                        }// end of if..
                    }// end of for..
                }// end of else
            }
		
            // I already know the shared key
            if(desKey != null)
            {


                if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_DESKEY_IS_NOT_NULL_I_KNOW_IT);
                try
                {
                    // if the message is not empty, encrypt it
                    if(msg.getBuffer() != null)
                    {
                        cipher.init(Cipher.ENCRYPT_MODE, desKey);
                        msg.setBuffer(cipher.doFinal(msg.getBuffer()));
                        msg.putHeader(EncryptHeader.KEY, new EncryptHeader(0));

                        if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_HAVE_DES_KEY__PACKAGE_SENT);
                    }
                    else
                    {
                        msg.setBuffer(null);
                        msg.putHeader(EncryptHeader.KEY, new EncryptHeader(0));

                        if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_BUFFER_NULL_ADDED_HEADER);
                    }
                }catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            break;
        }// check des key..

        if(log.isInfoEnabled()) log.info(ExternalStrings.ENCRYPT1_4_PASS_DOWN__0, evt.toString());
        passDown(evt);          // Pass on to the layer below us
    }

    private SecretKey decodedKey(byte[] encodedKey) {
        SecretKey key=null;
        try {
            //change needed mandar
            SecretKeyFactory KeyFac=SecretKeyFactory.getInstance(getAlgorithm(symAlgorithm));
            SecretKeySpec desKeySpec=new SecretKeySpec(encodedKey, getAlgorithm(symAlgorithm));
            key=KeyFac.generateSecret(desKeySpec);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return key;
    }

    private PublicKey generatePubKey(byte[] encodedKey) {
        PublicKey tmpPubKey=null;
        try {
            KeyFactory KeyFac=KeyFactory.getInstance(getAlgorithm(asymAlgorithm));
            X509EncodedKeySpec x509KeySpec=new X509EncodedKeySpec(encodedKey);
            tmpPubKey=KeyFac.generatePublic(x509KeySpec);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return tmpPubKey;
    }
}
