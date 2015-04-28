/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.persistence;

/**
 * @author Mandar Shinde
 * This class is the factory to get access to any DB based or file based
 * implementation. None of the implementations should expose directly
 * to user for migration purposes
 */


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

@SuppressFBWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",justification="GemFire doesn't use this class")
public class PersistenceFactory
  {

    protected final static GemFireTracer log=GemFireTracer.getLog(PersistenceFactory.class);

    /**
     * Default private constructor// does nothing
     */
    private PersistenceFactory()
    {
    }


    /**
     * Singular public method to get access to any of the Persistence
     * Manager implementations. It is important to known at this point
     * that properties determine the implementation of the Persistence
     * Manager, there is no direct interface which gives access to 
     * either DB implemented ot FILE implemented storage API.
     * @return PersistenceFactory;
     */
    public static PersistenceFactory getInstance() {
        log.error(ExternalStrings.PersistenceFactory_GETTING_FACTORY_INSTANCE);
        if (_factory == null)
            _factory = new PersistenceFactory();
        return _factory;
    }

    /**
     * Register a custom persistence manager as opposed to the
     * {@link FilePersistenceManager} or {@link DBPersistenceManager}.
     */ 
    public synchronized void registerManager(PersistenceManager manager)
    {
        _manager = manager;
    }

    /**
     * Reads the default properties and creates a persistencemanager
     * The default properties are picked up from the $USER_HOME or 
     * from the classpath. Default properties are represented by
     * "persist.properties"
     * @return PersistenceManager
     * @exception Exception;
     */ 
    public synchronized PersistenceManager createManager() throws Exception {
        // will return null if not initialized
        // uses default properties
        if (_manager == null)
        {
            if (checkDB())
                _manager = createManagerDB(propPath);
            else
                _manager = createManagerFile(propPath);
        }
        return _manager;
    }


    /**
     * Duplicated signature to create PersistenceManager to allow user to
     * provide property path. 
     * @param filePath complete pathname to get the properties
     * @return PersistenceManager;
     * @exception Exception;
     */
    public synchronized PersistenceManager createManager (String filePath) throws Exception 
    {
        if (_manager == null)
        {
            if (checkDB(filePath))
                _manager = createManagerDB(filePath);
            else
                _manager = createManagerFile(filePath);
        }
        return _manager;
    }



    /**
     * Internal creator of DB persistence manager, the outside user accesses only
     * the PersistenceManager interface API
     */
    private PersistenceManager createManagerDB(String filePath) throws Exception
    {

            if(log.isInfoEnabled()) log.info(ExternalStrings.PersistenceFactory_CALLING_DB_PERSIST_FROM_FACTORY__0, filePath);
        if (_manager == null)
            _manager = new DBPersistenceManager(filePath);
        return _manager;
    }// end of DB persistence

    /**
     * creates instance of file based persistency manager
     * @return PersistenceManager
     */
    private PersistenceManager createManagerFile(String filePath)
    {

            if(log.isInfoEnabled()) log.info(ExternalStrings.PersistenceFactory_CREATING_FILE_MANAGER__0, filePath);
        Properties props;

        try
        {
            if (_manager == null)
            {
                props=readProps(filePath);
                String classname=props.getProperty(filePersistMgr);
                if(classname != null)
                {
                    Class cl=Util.loadClass(classname, this.getClass());
                    Constructor ctor=cl.getConstructor(new Class[]{String.class});
                    _manager=(PersistenceManager)ctor.newInstance(new Object[]{filePath});
                }
                else
                {
                    _manager = new FilePersistenceManager(filePath);
                }
            }
            return _manager;
        }
        catch (Exception t)
        {
            t.printStackTrace();
            return null;
        }
    }// end of file persistence
    

    /**
     * checks the default properties for DB/File flag
     * @return boolean;
     * @exception Exception;
     */
    private boolean checkDB() throws Exception
    {
        Properties props=readProps(propPath);
        String persist = props.getProperty(persistProp);
        if ("DB".equals(persist))
            return true;
        return false;
    }




    /**
     * checks the provided properties for DB/File flag
     * @return boolean;
     */
    private boolean checkDB(String filePath) throws Exception
    {
        Properties props=readProps(filePath);
        String persist = props.getProperty(persistProp);
        if ("DB".equals(persist))
            return true;
        return false;
    }


    Properties readProps(String fileName) throws IOException
    {
        Properties props;
        FileInputStream _stream = new FileInputStream(fileName);
        props=new Properties();
        props.load(_stream);
        return props;
    }

    private static PersistenceManager _manager = null;
    private static PersistenceFactory _factory = null;
   

    /* Please set this according to configuration */
    final static String propPath = "persist.properties";
    final static String persistProp = "persist";

    /** The class that implements a file-based PersistenceManager */
    final static String filePersistMgr="filePersistMgr";
}
