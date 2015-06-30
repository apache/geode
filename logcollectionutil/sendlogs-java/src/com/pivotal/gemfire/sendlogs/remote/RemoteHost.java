package com.pivotal.gemfire.sendlogs.remote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.pivotal.gemfire.sendlogs.utilities.CloseGracefully;
import com.pivotal.gemfire.sendlogs.utilities.SendlogsExceptionHandler;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.SCPInputStream;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

/**
 * Maintains the connection to the remote hosts.  Provides the "lower level" interfaces to working with the 
 * remote host.  EG. logging in, executing commands, copying files, etc.
 * @author ablakema
 */
public class RemoteHost {
	private final String username;
	private final String hostname;
	private final AuthType authType;
	private final Logger logger = Logger.getLogger(RemoteHost.class.getName());

	private Connection conn = null;
	private Session sess = null;
	private String password = null;
	private File sshIdentityFileLocation = null;

	private enum AuthType {
		PASSWORD, PUBKEY
	}

	/**
	 * @param username Username used for SSH communications.
	 * @param hostname Remote hostname
	 * @param password Obtained from the user via STDIN.
	 */
	public RemoteHost(String username, String hostname, String password) {
		this.username = username;
		this.password = password;
		this.hostname = hostname;
		this.authType = AuthType.PASSWORD;
	}

	/**
	 * @param username Username used for SSH communications.
	 * @param hostname Remote hostname
	 * @param pubkeyLocation Obtained from the user via command line option.
	 */
	public RemoteHost(String username, String hostname, File pubkeyLocation) {
		this.username = username;
		this.hostname = hostname;
		this.sshIdentityFileLocation = pubkeyLocation;

		this.authType = AuthType.PUBKEY;
	}

	/**
	 * Uses the system default public keys to try authentication.
	 * @param username Username used for SSH communications.
	 * @param hostname Remote hostname
	 */
	public RemoteHost(String username, String hostname) {
		this.username = username;
		this.hostname = hostname;

		this.authType = AuthType.PUBKEY;

		sshIdentityFileLocation = getValidSshPrivKey();
	}

	/**
	 * @return Location of first available ssh key.
	 */
	private File getValidSshPrivKey() {
		/* TODO: Implement a way of *trying* both of these */
		String homeDir = System.getProperty("user.home");
		File rsaFile = new File(String.format("%s/.ssh/id_rsa", homeDir));
		File dsaFile = new File(String.format("%s/.ssh/id_dsa", homeDir));

		/* Default to checking for id_rsa because it's better. */
		if (rsaFile.isFile()) {
			return rsaFile;
		} else if (dsaFile.isFile()) {
			return dsaFile;
		}
		return null;
	}

	/**
	 * Tries to connect to the remote host via ssh.
	 * 
	 * This currently has a long timeout ~45 seconds.  This may need to be reduced.
	 * @return true if the connection was successful.
	 */
	private boolean connect() {
		boolean isAuthenticated = false;
		/* If we've already connected use that connection */
		if (conn != null && conn.isAuthenticationComplete()) return true;
		
		try {
			conn = new Connection(hostname);
			conn.connect();

			if (authType == AuthType.PASSWORD) {
				isAuthenticated = conn.authenticateWithPassword(username,
						password);
			} else if (sshIdentityFileLocation != null) {
				isAuthenticated = conn.authenticateWithPublicKey(username,
						sshIdentityFileLocation, "ignoreBecauseNotImplemented");
			} else {
				logger.warn("No valid credentials provided for " + hostname);
			}

			if (Boolean.FALSE.equals(isAuthenticated)) {
				logger.error("Authentication failed for " + hostname);
			} else {
				return true;
			}
		} catch (Exception e) {
			SendlogsExceptionHandler.handleException(e);
			conn.close();
		}
		return isAuthenticated;
	}

	/**
	 * Copies over remote file to the local working directory. 
	 * 
	 * @param remoteFilename Full path to the remote file.
	 * @param baseDir Base directory where the file is to be copied.
	 */
	public void getRemoteFile(String remoteFilename, String baseDir) {
		SCPInputStream scpIs = null;
		try {
			if (connect()) {
				String localFilename = baseDir + "/" + new File(remoteFilename).getName();
				SCPClient scp = conn.createSCPClient();
				FileOutputStream fos = new FileOutputStream(localFilename);
				scpIs = scp.get(remoteFilename);
				IOUtils.copy(scpIs, fos);
			}
		} catch (IOException e) {
			SendlogsExceptionHandler.handleException(e);
		}finally {
			if (scpIs != null ) CloseGracefully.close(scpIs);
		}
	}

	/**
	 * 
	 * @param command Executes specified command on the remote host that this instance is connected to.
	 * @return String with the results of command.  This is newline delimited.
	 */
	public String executeCommand(String command) {
		logger.debug("Executing command: " + command + " on host " + hostname);
		BufferedReader br = null;
		StringBuilder data = new StringBuilder();
		try {
			if (connect()) {
				sess = conn.openSession();
				sess.execCommand(command);
				InputStream stdout = new StreamGobbler(sess.getStdout());
				br = new BufferedReader(new InputStreamReader(stdout));
				while (true) {
					String line = br.readLine();
					if (line == null)
						break;
					data.append(line);
                    data.append("\n");
				}
			}
		} catch (Exception e) {
			SendlogsExceptionHandler.handleException(e);
		} finally {
			if (sess != null) sess.close();
			if (br != null) CloseGracefully.close(br);
		}
		return data.toString();
	}

	/**
	 * @return The hostname defined for this instance of {@link RemoteHost}.
	 */
	public String getHostname() {
		return hostname;
	}
	
	/**
	 * Close the connection to the SSH server.
	 */
	public void closeConnection() {
		conn.close();
	}
}
