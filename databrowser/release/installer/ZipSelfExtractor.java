/* ZipSelfExtractor.java */

import java.io.*;
import java.net.*;
import java.util.zip.*;
import java.util.*;
import java.util.jar.*;

/**
 * The abstract base for GemFire like products. This class 
 * needs to be extend to provide product specific 
 * names/functionality. 
 * @see {@link createJars}
 * @see {@link getInstallDirProperty}
 * @see {@link getInstallOpenSource}
 * @see {@link getProductJarName}
 * 
 * The following support files may also need to be customized.
 * @see i18n.properties
 * @see EULA.txt
 * @author kbanks
 */
public abstract class ZipSelfExtractor
{
  private boolean isInteractive;

  /** This must match the name used in build.xml#build-installer */
  private final static String OSS_JAR          = "oss.jar"; 
  private final static String OSS_PATTERN      = ".*/opensource/.*";
  
  private static int FILES_PER_DOT = 5;
  private static final int YES = 1;
  private static final int YES_TO_ALL = 2;
  private static final int NO = 3;
  private static final int CANCEL = 4;
  protected File installDir;
  private PropertyResourceBundle rb;
  private Throwable error;

  private static final String MANIFEST = "META-INF/MANIFEST.MF";
  private static final String EULA = "EULA.txt";
  private static final String I18N = "i18n";
 
  /** A series of calls to {@link createJar} */
  protected abstract void createJars() throws IOException;
  /** @return the name of the property that specifies the 
   * installation directory for an unattended installation */
  protected abstract String getInstallDirProperty();
  /** @return the name of the boolean property that controls 
   * opensource installation */
  protected abstract String getInstallOpenSourceProperty();
  
  /** @return the name of the jar that contains the product */
  protected abstract String getProductJarName();
  
  private void setError(Throwable t) { 
    if(error == null) {
      error = t;
    }
  } 
  
  private void handleFailure() throws Throwable {
    if (error != null) {
      if ( error instanceof InstallationCancelled ) {
        System.out.println("");
        Throwable cause = error.getCause();
        if (cause != null) { 
          //intentionally not getLocalizedMessage()
          System.out.println(cause.getMessage());
        }
        //intentionally not getLocalizedMessage()
        System.out.println(error.getMessage());
        System.exit(1);
      }
      System.out.println("");
      System.out.println(rb.getString("INSTALLATION_FAILED"));
      System.out.println(error.getLocalizedMessage());
      throw error;
     }
  }

  protected ZipSelfExtractor() {
    String directory = System.getProperty( getInstallDirProperty(), ".");
    if (System.getProperty(getInstallDirProperty(), null) == null) {
      isInteractive = true;
    } else {
      // User specified an install directory as a system Property
      // Assuming unattended installation.
      isInteractive = false;
    } 
    installDir = new File(directory);
    rb = (PropertyResourceBundle) ResourceBundle.getBundle(I18N, Locale.getDefault());
  }

  private String getJarFileName() {
    String myClassName = this.getClass().getName() + ".class";
    URL urlJar = ClassLoader.getSystemResource(myClassName);
    String urlStr = null;
    try {
      urlStr = URLDecoder.decode(urlJar.toString(), "UTF-8");
    } catch ( UnsupportedEncodingException uee ) {
      //This should never happen because UTF-8 is required to be implemented
      throw new RuntimeException(uee);
    }
    int from = "jar:file:".length();
    int to = urlStr.indexOf("!/");
    String jarName = urlStr.substring(from, to);
    return jarName; 
  }

  private boolean getInstallOpenSource() {
	return Boolean.getBoolean(getInstallOpenSourceProperty());
  }

  private void extract(String zipfile) {
    System.out.println(rb.getString("BEGIN"));
    File currentArchive = new File(zipfile);
    displayEULA();
    queryInstallDir();
    byte[] buf = new byte[1024];
    boolean overwrite = false;

    ZipFile zf = null;
    FileOutputStream out = null;
    InputStream in = null;

    try {
       zf = new ZipFile(currentArchive);
       int size = zf.size();
       int extracted = 0;
       System.out.print("");
       System.out.print(rb.getString("UNZIPPING_FILES"));
       Enumeration entries = zf.entries();

       String innerClassName = 
         InstallationCancelled.class.getName() + ".class";
       for (int i=0; i<size; i++) {
         ZipEntry entry = (ZipEntry) entries.nextElement();
         if(entry.isDirectory())
           continue;

         String pathname = entry.getName();
         if( pathname.endsWith("ZipSelfExtractor.class") 
            || innerClassName.equals(pathname)
            || MANIFEST.equals(pathname.toUpperCase())
            || pathname.startsWith(I18N) 
            || ((! getInstallOpenSource()) && pathname.matches(OSS_PATTERN))) {
           continue;
         }

         extracted ++;
         if ( extracted % FILES_PER_DOT == 0 ) {
           System.out.print(".");
         }

         in = zf.getInputStream(entry);

         File outFile = new File(installDir, pathname);
         Date archiveTime = new Date(entry.getTime());

         if(overwrite==false) {
           if(outFile.exists()) {
             int result = overwrite(outFile);
                            
             if(result == NO) {
               continue;
             } else if(result == YES_TO_ALL) {
               overwrite = true;
             } else if(result == CANCEL) {
               throw new InstallationCancelled(rb.getString("CANCEL_INSTALL"));
             } else if(result == YES) {
               //no action required
             }
           }
         }

         File parent = new File(outFile.getParent());
         if (parent != null && !parent.exists()) {
           parent.mkdirs();
         }

         out = new FileOutputStream(outFile);                

         while (true) {
           int nRead = in.read(buf, 0, buf.length);
           if (nRead > 0) {
             out.write(buf, 0, nRead);
           } else {
             break;
           }
         }
                    
         out.close();
         outFile.setLastModified(archiveTime.getTime());
       }
             
       zf.close();

       File productDir = getProductDir();
       File binDir = new File(productDir, "bin");
       setPermissions(binDir.getAbsolutePath());

       if ( getInstallOpenSource() ) {
         System.out.println();
         System.out.println(rb.getString("INSTALLING_OPENSOURCE"));

         File libDir = new File(productDir, "lib");
         File productArchive = new File(libDir, getProductJarName());
         installOpenSource(productArchive);

         File ossArchive = new File(getOpenSourceDir(), OSS_JAR);
         installOpenSource(ossArchive);
         createJars();
         cleanUpOpenSourceDir();
       }
     } catch (IOException ioe) {
       setError(ioe);
       if(zf!=null) { try { zf.close(); } catch(IOException ignore) {;} }
       if(out!=null) { try {out.close();} catch(IOException ignore) {;} }
       if(in!=null) { try { in.close(); } catch(IOException ignore) {;} }
       throw new RuntimeException(rb.getString("INPUT_ERROR"), ioe);
     }
     System.out.println("");
     System.out.println(rb.getString("INSTALLATION_COMPLETE"));
  }

  private void displayEULA() {
    if (isInteractive) {
      ClassLoader loader = this.getClass().getClassLoader();
      BufferedReader stream = null;
      stream = new BufferedReader(new InputStreamReader(loader.getResourceAsStream(EULA)));
      String line = null;
      int count = 20;
      try {
        while (true) {
          if ( count <= 0 ) {
            System.out.println(""); 
            System.out.println(rb.getString("CONTINUE")); 
            System.out.flush();
            System.in.read();
            count = 20;
          }
          line = stream.readLine();
          count--;
          if (line != null) {
            System.out.println(line);
          } else {
            break;
          }
        }
        stream.close();
      } catch (IOException ioe) {
        setError(ioe);
        throw new RuntimeException(rb.getString("EULA_ERROR"), ioe);
      }
      String response = "";
      final String agree = rb.getString("AGREE").toUpperCase();
      final String disagree = rb.getString("DISAGREE").toUpperCase();
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      boolean flag = false;
      while(! response.trim().toUpperCase().startsWith(agree)) {
        if ( flag ) {
          System.out.println(rb.getString("BE_LITERAL"));
        }
        flag = true;
        if ( response.trim().toUpperCase().startsWith(disagree) ) {
          throw new InstallationCancelled(rb.getString("CANCEL_INSTALL"));
        }
        System.out.println(""); 
        System.out.println(rb.getString("AGREE_TO_EULA"));
        try {
          response = br.readLine();
        } catch (IOException ioe) {
          setError(ioe);
          throw new RuntimeException(rb.getString("INPUT_ERROR"), ioe);
        }
      }
    }
  }

  private void queryInstallDir() {
    boolean directoryIsChoosen = false;
    if (! isInteractive) {
      directoryIsChoosen = true;
      validateInstallDir();
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String response = "";
    while (! directoryIsChoosen) {
      System.out.println("");
      System.out.println(rb.getString("CHOOSE_AN_INSTALL_DIRECTORY"));
      String path;
      try {
        path = installDir.getCanonicalPath();
      } catch (IOException ioe) {
        path = installDir.getPath();
      }
      System.out.println("[" + path + "]");
      try {
        response = br.readLine();
        if(! response.trim().equals("")) {
          installDir = new File(response.trim());
        }
      } catch (IOException ioe) {
        setError(ioe);
        throw new RuntimeException(rb.getString("INPUT_ERROR"), ioe);
      }
      directoryIsChoosen = validateInstallDir();
    }
  }

  private boolean validateInstallDir() {
    String path;
    try {
      path = installDir.getCanonicalPath();
    } catch (IOException ioe) {
      path = installDir.getAbsolutePath();
    }
    if (installDir.exists()) {
      System.out.print(rb.getString("ACCEPT_INSTALL_DIRECTORY") + " ");
      System.out.println(path + " [" + rb.getString("YES") + "]");
    } else {
      System.out.println(rb.getString("DIRECTORY_DOES_NOT_EXIST_ACCEPT") 
        + " [" + rb.getString("YES") + "]");
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String response = "";
    try {
      if (isInteractive) {
        response = br.readLine();
      }
      response = response.trim().toUpperCase();
      final String yes = rb.getString("YES").toUpperCase();
      if( response.equals("")
          || response.startsWith(yes)) {
        if (! installDir.exists()) {
          if (! installDir.mkdirs()) {
            if (isInteractive) {
              System.out.println(rb.getString("FAILED_TO_CREATE_DIRECTORY"));
              return false;
            } else {
               IOException ioe = new IOException(
                 rb.getString("FAILED_TO_CREATE_DIRECTORY"));
               throw new InstallationCancelled(
                 rb.getString("CANCEL_INSTALL"), ioe);
            }
          }
        }
        if (! installDir.canWrite()) {
          if (isInteractive) {
            System.out.println(rb.getString("DIRECTORY_LACKS_PERMISSIONS"));
            return false;
          } else {
            IOException ioe = new IOException(rb.getString("DIRECTORY_LACKS_PERMISSIONS"));
            throw new InstallationCancelled(rb.getString("CANCEL_INSTALL"), ioe);
          }
        }
        return true;
      }
    } catch (IOException ioe) {
      setError(ioe);
      throw new RuntimeException(rb.getString("INPUT_ERROR"), ioe);
    }
    return false;
  }
  
  private int overwrite(File outFile) {
    if (! isInteractive) {
      String msg = outFile.getPath() + ": " + rb.getString("FILE_CONFLICT");
      IOException ioe = new IOException(msg);
      throw new InstallationCancelled(rb.getString("CANCEL_INSTALL"), ioe);
      //play it safe and exit if the files exist
    }

    System.out.println("\n");
    System.out.println(rb.getString("FILE_CONFLICT"));
  
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String response;
    while(true) {
      System.out.println(outFile.getPath() + ":\n" 
        + rb.getString("OVERRWRITE"));
      try {
        response = br.readLine();
      } catch (IOException ioe) {
        setError(ioe);
        throw new RuntimeException(rb.getString("INPUT_ERROR"), ioe);
      }
      try {
        int choice = Integer.parseInt(response); 
        if ( choice == YES 
          || choice == YES_TO_ALL
          || choice == NO 
          || choice == CANCEL) {
          return choice;
        } 
      } catch( NumberFormatException ignore ) {
      }
    }
  }

  private void cleanUpOpenSourceDir() throws IOException {
    File openSourceDir = getOpenSourceDir();
    File[] files = openSourceDir.listFiles();
    // files should never be null because of the lib directory
    for(int i=0; i < files.length; i++) {
      String name = files[i].getName();
      if( files[i].isDirectory() 
          && (! (name.equals("lib") || name.equals("source")))) {
        deleteDirectory(files[i]);
      } else if( name.equals(OSS_JAR)) {
        files[i].delete();
      } 
    } 
  }

  protected void createJar(String jarName, String pattern) 
    throws IOException
  {
    File openSourceDir = getOpenSourceDir();
    File openSourceLibDir = new File(getOpenSourceDir(), "lib");
    openSourceLibDir.mkdir();
    File jarFile = new File(openSourceLibDir.getPath(), jarName);
    JarOutputStream jar = new JarOutputStream (  
      new FileOutputStream(jarFile),  new Manifest()); 
    jarFileTree(jar, openSourceDir, openSourceDir, pattern);     
    jar.close();
  }

  private void jarFileTree ( 
    JarOutputStream jar,
    File file, 
    File openSourceDir,
    String pattern ) throws FileNotFoundException, IOException
  {
    if ( file.isDirectory() ) {
      File[] search = file.listFiles();
      if ( search == null ) {
        return; //empty directory 
      }
      for( int i=0; i < search.length; i++) {
        jarFileTree(jar, search[i], openSourceDir, pattern); 
      } 
    } else {
      String path = file.getPath();
      if (! path.matches(pattern)) {
        return; //Not in our package of interest
      } 
      byte[] buffer = new byte[1024]; 
      int count; 
      //File sourceFile = new File(openSourceDir, file.getPath());
      InputStream is = new BufferedInputStream(new FileInputStream(file)); 
      int index = openSourceDir.getPath().length() + 1;
      String entryName = file.getPath().substring(index);
      JarEntry entry = new JarEntry(entryName); 
      jar.putNextEntry(entry); 
 
      // Read the file the file and write it to the jar. 
      while((count = is.read(buffer)) != -1) {  
        jar.write ( buffer, 0, count ) ; 
      }  
    }  
  }

  private File getProductDir() throws FileNotFoundException {
    File productDir = new File(installDir, rb.getString("PRODUCT_DIR"));
    if ( ! productDir.exists() || ! productDir.isDirectory() ) {
      throw new FileNotFoundException(rb.getString("PRODUCT_DIR_NOT_FOUND"));
    }
    return productDir;
  }
 
  private File getOpenSourceDir() throws FileNotFoundException {
    return new File(getProductDir(), "opensource");
  }

  private void installOpenSource(File currentArchive) throws IOException {
    File openSourceDir = getOpenSourceDir();
    byte[] buf = new byte[1024];
 
    ZipFile zf = null;
    FileOutputStream out = null;
    InputStream in = null;

    try {
       zf = new ZipFile(currentArchive);
       int size = zf.size();
       int extracted = 0;
       System.out.println("");
       System.out.print(rb.getString("UNZIPPING_FILES"));
       Enumeration entries = zf.entries();

       for (int i=0; i<size; i++) {
         ZipEntry entry = (ZipEntry) entries.nextElement();
         if(entry.isDirectory())
           continue;

         String pathname = entry.getName();

         extracted ++;
         if ( extracted % FILES_PER_DOT == 0 ) {
           System.out.print(".");
         }

         in = zf.getInputStream(entry);
         File outFile = new File(openSourceDir, pathname);
         Date archiveTime = new Date(entry.getTime());

         File parent = new File(outFile.getParent());
         if (parent != null && !parent.exists()) {
           parent.mkdirs();
         }

         out = new FileOutputStream(outFile);                

         while (true) {
           int nRead = in.read(buf, 0, buf.length);
           if (nRead > 0) {
             out.write(buf, 0, nRead);
           } else {
             break;
           }
         }
                    
         out.close();
         outFile.setLastModified(archiveTime.getTime());
       }
             
       zf.close();
     } catch (IOException ioe) {
       setError(ioe);
       if(zf!=null) { try { zf.close(); } catch(IOException ignore) {;} }
       if(out!=null) { try {out.close();} catch(IOException ignore) {;} }
       if(in!=null) { try { in.close(); } catch(IOException ignore) {;} }
       throw new RuntimeException(rb.getString("INPUT_ERROR"), ioe);
     }
  }
  
  static private boolean deleteDirectory(File path) {
    if( path.exists() ) {
      File[] files = path.listFiles();
      if ( files == null ) {
        return( path.delete() ); //empty directory
      }
      for(int i=0; i<files.length; i++) {
         if(files[i].isDirectory()) {
           deleteDirectory(files[i]);
         } else {
           files[i].delete();
         }
      }
    }
    return( path.delete() );
  }
  
  protected void installProduct() throws Throwable {
    try {
      String jarFileName = getJarFileName();
	  extract(jarFileName);
	} catch( Throwable t ) {
	  setError(t);
	} finally {
	  handleFailure();
	}
  }
  
  static class InstallationCancelled extends RuntimeException {
    private static final long serialVersionUID = 7801293643025052283L;
    public InstallationCancelled() {super();}
    public InstallationCancelled(String msg) {super(msg);}
    public InstallationCancelled(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  private static void addFiles(String binDir, List command) {
    File dir = new File(binDir);
    File[] children = dir.listFiles();
    if (children == null) {
        // Either dir does not exist or is not a directory
    } else {
        for (int i=0; i<children.length; i++) {
            if ( children[i].isFile() ) {
              command.add(children[i].getAbsolutePath());
            }
        }
    }
  }

  private void setPermissions(String binDir) {
    String OS = System.getProperty("os.name", "UNSET").toLowerCase();
    final List command = new ArrayList();
    if ( OS.indexOf("windows") == -1 ) {
      //not on Windows, assume chmod
      command.add("chmod");
      command.add("755");
      addFiles(binDir, command);
    } else {
      /*
      command.add("cmd.exe");
      command.add("/c");
      command.add("cacls.exe");
      addFiles(binDir, command);
      command.add("/E");
      command.add("/C");
      command.add("/E");
      command.add("/E");
      */
      //Do nothing on Windows for now
      return;
    }
    String[] cmd = new String[command.size()];
     command.toArray(cmd);
    int retVal = -1;
    Process p = null;
    try {
      p = Runtime.getRuntime().exec ( cmd );
      retVal = p.waitFor();
    } catch (Exception suppress) {
    } finally {
      if(p != null) p.destroy();
    }
    if ( retVal != 0 ) {
      System.out.println(rb.getString("FAIL_TO_SET_PERMISSIONS"));
    }
  }
}
