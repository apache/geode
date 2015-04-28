/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package  com.gemstone.org.jgroups.persistence;

/**
 * @author Mandar Shinde
 * The class implements the PersistenceManager interface and provides users
 * a file based implementation when required.
 * The state of this class is current NOOP. Implementation will be in place 
 * once a better structure for file based properties will be designed.
 */


import java.io.*;
import java.util.*;

public class FilePersistenceManager implements PersistenceManager
{
    private final File file;

    /**
     * Default constructor
     */
    public FilePersistenceManager(String propertiesFilename)
        throws Exception
    {
        Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFilename));
        String path = properties.getProperty(PersistenceFactory.persistProp);
        file = new File(path);
        if (!file.createNewFile()) {
          throw new IOException("Unable to create file " + file.getAbsolutePath()); // GemStoneAddition
        }
    }

    /**
     * Save new NV pair as serializable objects or if already exist; store 
     * new state 
     */
    public void save(Serializable key, Serializable val) throws CannotPersistException
    {
        try
        {
            Map map = retrieveAll();
            map.put(key, val);
            saveAll(map);
        }
        catch (CannotRetrieveException e)
        {
            throw new CannotPersistException(e, "Unable to pre-load existing store.");
        }
    }

    /**
     * Remove existing NV from being persisted
     */
    public Serializable  remove(Serializable key) throws CannotRemoveException
    {
        Object o;
        try
        {
            Map map = retrieveAll();
            o = map.remove(key);
            saveAll(map);
        }
        catch (CannotRetrieveException e)
        {
            throw new CannotRemoveException(e, "Unable to pre-load existing store.");
        }
        catch (CannotPersistException e)
        {
            throw new CannotRemoveException(e, "Unable to pre-load existing store.");
        }
        return (Serializable) o;
    }


    /**
     * Use to store a complete map into persistent state
     * @exception CannotPersistException;
     */
    public void saveAll(Map map) throws CannotPersistException
    {
        try
        {
            OutputStream fos = new FileOutputStream(file);
            Properties prop = new Properties();
            // NB: For some reason Properties.putAll(map) doesn't seem to work - dimc@users.sourceforge.net
            for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();)
            {
                Map.Entry entry = (Map.Entry) iterator.next();
                prop.setProperty(entry.getKey().toString(), entry.getValue().toString());
            }
            prop.store(fos, null);
            fos.flush();
            fos.close();
        }
        catch (IOException e)
        {
            throw new CannotPersistException(e, "Cannot save to: " + file.getAbsolutePath());
        }
    }


    /**
     * Gives back the Map in last known state
     * @return Map;
     * @exception CannotRetrieveException;
     */
    public Map retrieveAll() throws CannotRetrieveException
    {
        try
        {
            Properties prop = new Properties();
            FileInputStream fis = new FileInputStream(file);
            prop.load(fis);
            fis.close();
            return filterLoadedValues(prop);
        }
        catch (IOException e)
        {
            throw new CannotRetrieveException(e, "Unable to load from file: " + file.getAbsolutePath());
        }
    }

    /**
     * Turns the values into Floats to enable
     * <code>com.gemstone.org.jgroups.demos.DistributedHashtableDemo</code> to work. 
     * Subclasses should override this method to convert the incoming map
     * of string/string key/value pairs into the types they want.  
     * @param in
     * @return Map
     */
    protected Map filterLoadedValues(Map in)
    {
        Map out = new HashMap();
        for (Iterator iterator = in.entrySet().iterator(); iterator.hasNext();)
        {
            Map.Entry entry = (Map.Entry) iterator.next();
            out.put(entry.getKey().toString(), Float.valueOf(entry.getValue().toString()));
        }
        return out;
    }


    /**
     * Clears the complete NV state from the DB
     * @exception CannotRemoveException;
     x*/
    public void clear() throws CannotRemoveException
    {
        try
        {
            saveAll(Collections.EMPTY_MAP);
        }
        catch (CannotPersistException e)
        {
            throw new CannotRemoveException(e, "Unable to clear map.");
        }
    }


    /**
     * Used to handle shutdown call the PersistenceManager implementation. 
     * Persistent engines can leave this implementation empty.
     */
    public void shutDown()
    {
	return;
    }
}// end of class
