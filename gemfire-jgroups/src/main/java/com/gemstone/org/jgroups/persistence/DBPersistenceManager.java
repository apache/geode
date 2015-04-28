/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.persistence;

/**
 * @author Mandar Shinde
 * This class implements the DB storage pattern for the Persistence
 * Manager interface. The implementation is open and can be used (and
 * tested) over more than one databases. It uses a string (VARCHAR)
 * as the key and either BLOB or VARBINARY db-datatype for the
 * serialized objects. THe user has the option to choose his/her own
 * schema over the ones provided.
 */


import java.io.Serializable;
import java.util.Map;

import com.gemstone.org.jgroups.util.GemFireTracer;


/**
 * Class will be utilized
 */
public class DBPersistenceManager implements PersistenceManager {

    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());

    @Override
    public void save(Serializable key, Serializable val)
        throws CannotPersistException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Serializable remove(Serializable key) throws CannotRemoveException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void saveAll(Map map) throws CannotPersistException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Map retrieveAll() throws CannotRetrieveException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void clear() throws CannotRemoveException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void shutDown() {
      // TODO Auto-generated method stub
      
    }

//    /**
//     * Default construct
//     * @param filename absolute filepath
//     * @exception Exception;
//     */
    public DBPersistenceManager(String filename) throws Exception {
//        String home_dir = null;
//
//        // PropertyPermission not granted if running in an untrusted environment with JNLP.
//        try {
//            home_dir = System.getProperty("user.home");
//        }
//        catch (SecurityException ex1) {
//        }
//
//        // 1. Try ${user.home}/persist.properties
//        try {
//            home_dir=home_dir + '/' + filename;
//            init(new FileInputStream(home_dir));
//            return;
//        }
//        catch(Exception ex) {
//            ;
//        }
//
//        // 2. Try to find persist.properties from somewhere on the CLASSPATH
//        try {
//            InputStream in = ClassPathLoader.getLatest().getResourceAsStream(getClass(), '/' + filename);
//            if(in != null) {
//                init(in);
//                return;
//            }
//        }
//        catch(Exception x) {
//            if(log.isErrorEnabled()) log.error(JGroupsStrings.DBPersistenceManager_FAILED_READING_DATABASE_PROPERTIES_FROM__0__EXCEPTION_1, new Object[] {filename, x});
//        }
//
//        // 3. Finally maybe the user specified -Dpersist.properties=/home/user/mypersist.properties
//        try {
//            home_dir=System.getProperty("persist.properties");
//            init(new FileInputStream(home_dir));
//            return;
//        }
//        catch(Exception ex) {
//            ;
//        }
//
//        // 4. If none of the above helped us to find persist.properties, give up and throw an exception
//        throw new Exception("DBPersistenceManager.DBPersistenceManager(): " +
//                            "failed reading database properties from " + filename);
    }
//
//
//    /**
//     * Duplicate constructor allowing inputstream
//     * @param input
//     * @exception Exception
//     */
//    public DBPersistenceManager(InputStream input) throws Exception {
//        init(input);
//    }
//
//
//    /**
//     * used to intitiailize complete DB access. THis method will use
//     * existing database to create schema (if it doesnt exist) and
//     * get PersistenceManager in usable condition
//     * @param in
//     * @exception Exception;
//     */
//    protected void init(InputStream in) throws Exception {
//        list=new Vector();
//        readProps(in);
//        loadDriver();
//
//        //check conn
//        Connection conn=this.getConnection();
//        this.closeConnection(conn);
//        createDBTables();
//        retrieveAll(); // work around to make sure, no duplicates are created.
//        log.error(JGroupsStrings.DBPersistenceManager_DONE_CONSTRUCTING_DB_PERSIST_MANAGER);
//    }
//
//
//    // TODO list for this implementation
//    // add constructor for xml file
//    // add constructor for default
//
//
//    /**
//     * Saves NV pair as serializable object; 
//     * creates if new, stores new state if already exists.
//     * @param key
//     * @param val
//     * @exception CannotPersistException;
//     */
//    public void save(Serializable key, Serializable val) throws CannotPersistException {
//        // checking if this is update or new entry
//        if(!entryExists(key)) {
//            log.error(JGroupsStrings.DBPersistenceManager_ENTRY_DOESNT_EXIST_FOR__0, key.toString());
//            try {
//                addNewEntry(key, val);
//                list.add(key.toString());
//                return;
//            }
//            catch (VirtualMachineError err) { // GemStoneAddition
//              SystemFailure.initiateFailure(err);
//              // If this ever returns, rethrow the error.  We're poisoned
//              // now, so don't let this thread continue.
//              throw err;
//            }
//            catch(Throwable t1) {
//              SystemFailure.checkFailure(); // GemStoneAddition
//                t1.printStackTrace();
//                //trace here
//                throw new CannotPersistException(t1, " error adding a completely new entry in to DB ");
//            }
//        }// checking entries
//
//        // THis is for regular updates to the key,val pair
//        Connection conn=null;
//        PreparedStatement prepStat=null;
//        try {
//            conn=this.getConnection();
//            String keyStr=null;
//            keyStr=key.toString();
//            byte[] keyBytes=getBytes(key);
//            byte[] valBytes=getBytes(val);
//            log.error(JGroupsStrings.DBPersistenceManager_VALUE_IS__0, val);
//            //use simple execute, do not create prepared statement
//            prepStat=conn.prepareStatement(updateStat);
//            prepStat.setString(3, keyStr);
//            prepStat.setBytes(1, keyBytes);
//            prepStat.setBytes(2, valBytes);
//            prepStat.executeQuery();
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace here
//            t.printStackTrace();
//            // throw exception here
//            throw new CannotPersistException(t, "error updating an existing entry in to the database ");
//        }
//                // cleanup
//        finally {
//            try {
//                if(prepStat != null) prepStat.close();
//                this.closeConnection(conn);
//            }
//            catch (VirtualMachineError err) { // GemStoneAddition
//              SystemFailure.initiateFailure(err);
//              // If this ever returns, rethrow the error.  We're poisoned
//              // now, so don't let this thread continue.
//              throw err;
//            }
//            catch(Throwable t) {
//              SystemFailure.checkFailure(); // GemStoneAddition
//                // trace
//                conn=null;
//                prepStat=null;
//            }
//        }
//    }
//
//
//    /**
//     * Removes existing entry.
//     * @param key
//     * @exception CannotRemoveException;
//     */
//    public Serializable remove(Serializable key) throws CannotRemoveException {
//        Connection conn=null;
//        Statement stat=null;
//        PreparedStatement prepStat=null;
//        ResultSet set=null;
//        Serializable val=null;
//
//        try {
//            conn=this.getConnection();
//            stat=conn.createStatement();
//            String exQuery=" select * from replhashmap where key like '" + key.toString() + '\'';
//            set=stat.executeQuery(exQuery);
//            set.next();
//            val=getSerializable(set.getBinaryStream(3));
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t3) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace
//            t3.printStackTrace();
//            throw new CannotRemoveException(t3, " Error retrieving value for given key");
//        }
//        finally {
//            try {
//                if(stat != null) stat.close(); // GemStoneAddition wrong variable
//                this.closeConnection(conn);
//            }
//            catch (VirtualMachineError err) { // GemStoneAddition
//              SystemFailure.initiateFailure(err);
//              // If this ever returns, rethrow the error.  We're poisoned
//              // now, so don't let this thread continue.
//              throw err;
//            }
//            catch(Throwable t) {
//              SystemFailure.checkFailure(); // GemStoneAddition
//                // trace
//                conn=null;
////                prepStat=null; GemStoneAddition (redundant assignment)
//            }
//        }
//
//
//        try {
//            conn=this.getConnection();
//            prepStat=conn.prepareStatement(removeStat);
//            prepStat.setString(1, key.toString());
//            prepStat.executeQuery();
//            list.remove(key.toString());
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace here..
//            t.printStackTrace();
//            // throw Exception
//            throw new CannotRemoveException(t, "Could not remove existing entry due to error in jdbc transaction");
//        }
//
//                // cleanup
//        finally {
//            try {
//                set.close();
//                stat.close();
//                if(prepStat != null) prepStat.close();
//                this.closeConnection(conn);
//            }
//            catch (VirtualMachineError err) { // GemStoneAddition
//              SystemFailure.initiateFailure(err);
//              // If this ever returns, rethrow the error.  We're poisoned
//              // now, so don't let this thread continue.
//              throw err;
//            }
//            catch(Throwable t) {
//              SystemFailure.checkFailure(); // GemStoneAddition
//                // trace
//                conn=null;
//                stat=null;
//            }//end of try..catch
//        }// end of finally..
//        return val;
//    }// end of remove
//
//
//    /**
//     * Saves all row entries for the map to DB.
//     * @param map
//     * @exception CannotPersistException;
//     */
//    public synchronized void saveAll(Map map) throws CannotPersistException {
//        Iterator iter=null;
//        try {
////            Set keySet=map.keySet();
////            iter=keySet.iterator();
//            iter = map.entrySet().iterator(); // GemStoneAddition make more efficient
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            t.printStackTrace();
//            //trace here
//            throw new CannotPersistException(t, "Error with the map entered to saveAll");
//        }
//
//        //Individually saving all
//        while(iter.hasNext()) {
//            try {
////                Serializable key=(Serializable) iter.next();
////                Serializable val=(Serializable) map.get(key);
//                Map.Entry entry = (Map.Entry)iter.next(); // GemStoneAddition
//                Serializable key = (Serializable)entry.getKey();
//                Serializable val = (Serializable)entry.getValue();
//
//                // dont this in same thread, optimization can be added
//                this.save(key, val);
//            }
//            catch (VirtualMachineError err) { // GemStoneAddition
//              SystemFailure.initiateFailure(err);
//              // If this ever returns, rethrow the error.  We're poisoned
//              // now, so don't let this thread continue.
//              throw err;
//            }
//            catch(Throwable t2) {
//              SystemFailure.checkFailure(); // GemStoneAddition
//                t2.printStackTrace();
//                //trace here
//                continue;
//            }
//        }// end of while..
//    }// end of saveall
//
//
//    /**
//     * Used to retrieve the persisted map back to its last known state
//     * @return Map;
//     * @exception CannotRetrieveException;
//     */
//    public synchronized Map retrieveAll() throws CannotRetrieveException {
//        Connection conn=null;
//        Statement stat=null;
//        ResultSet set=null;
//        Map map=null;
//        try {
//            conn=this.getConnection();
//            stat=conn.createStatement();
//            set=stat.executeQuery(" select * from replhashmap");
//            map=retrieveAll(set);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace here
//            throw new CannotRetrieveException(t, "Error happened while querying the database for bulk retrieve, try starting DB manually");
//        }
//
//
//        //finally
//        try {
//            stat.close();
//            this.closeConnection(conn);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t1) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            // trace it
//            // ignore
//        }
//
//        return map;
//    }// end of retrieveall
//
//
//    /**
//     * Helper method to get get back the map
//     * @return Map;
//     * @exception Exception;
//     */
//    private Map retrieveAll(ResultSet result) throws Exception {
//        HashMap map=new HashMap();
//        while(result.next()) {
//            InputStream inputStrKey=result.getBinaryStream(2);
//            InputStream inputStrVal=result.getBinaryStream(3);
//            Serializable key=getSerializable(inputStrKey);
//            Serializable val=getSerializable(inputStrVal);
//            map.put(key, val);
//            list.add(key.toString());
//        }// end of while..
//        return map;
//    }
//
//
//    /**
//     * Clears the key-cache as well as all entries
//     * @exception CannotRemoveException;
//     */
//    public void clear() throws CannotRemoveException {
//        Connection conn=null;
//        Statement stat=null;
//        try {
//            conn=this.getConnection();
//            stat=conn.createStatement();
//            stat.executeQuery("delete from replhashmap");
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace here
//            throw new CannotRemoveException(t, " delete all query failed with existing database");
//        }
//
//        //finally
//        try {
//            stat.close();
//            this.closeConnection(conn);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            conn=null;
//            stat=null;
//        }
//    }
//
//
//    /**
//     * Shutting down the database cleanly
//     */
//    public void shutDown() {
//        // non-trivial problem, more research required
//        // no-op for now..
//    }
//
//
//
//    /**
//     * The private interfaces are used specifically to this manager
//     */
//
//    /**
//     * Used to enter a completely new row in to the current table
//     * @param key
//     * @param val
//     * @exception CannotPersistException;
//     */
//    private void addNewEntry(Serializable key, Serializable val) throws CannotPersistException, CannotConnectException {
//        Connection conn=getConnection();
//        try {
//            PreparedStatement prepStat=conn.prepareStatement(insertStat);
//            prepStat.setString(1, key.toString());
//            byte[] keyBytes=getBytes(key);
//            byte[] valBytes=getBytes(val);
//            //InputStream keyStream = getBinaryInputStream(key);
//            //InputStream valStream = getBinaryInputStream(val);
//            prepStat.setBytes(2, keyBytes);
//            prepStat.setBytes(3, valBytes);
//            //prepStat.setBinaryStream(keyStream);
//            //prepStat.setBinaryStream(valStream);
//            prepStat.executeQuery();
//            conn.commit();
//            log.error(JGroupsStrings.DBPersistenceManager_EXECUTING_INSERT__0, insertStat);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //conn.rollback();
//            t.printStackTrace();
//            //trace here
//            throw new CannotPersistException(t, "error adding new entry using creating Db connection and schema");
//        }
//    }// end of addentry..
//
//
//    /**
//     * Gets a binaryinputstream from a serialized object
//     * @param ser
//     * @return BinaryInputStream
//     * @exception Exception
//     */
//    private java.io.InputStream getBinaryInputStream(Serializable ser) throws Exception {
//        ByteArrayOutputStream stream=new ByteArrayOutputStream();
//        ObjectOutputStream keyoos=new ObjectOutputStream(stream);
//        keyoos.writeObject(ser);
//        ByteArrayInputStream pipe=new ByteArrayInputStream(stream.toByteArray());
//        return pipe;
//    }// end of stream conversion
//
//
//    /**
//     * Gets a serializable back from a InputStream
//     * @param stream
//     * @return Serializable
//     * @exception Exception
//     */
//    private Serializable getSerializable(java.io.InputStream stream) throws Exception {
//        ObjectInputStream ooStr=new ObjectInputStream(stream);
//        Serializable tmp=(Serializable) ooStr.readObject();
//        return tmp;
//    }
//
//
//    /**
//     * Used to enter a completely new row in to the current table
//     * @param key
//     * @param val
//     * @exception CannotPersistException
//     */
//    private void addNewEntryGen(Serializable key, Serializable val) throws CannotPersistException, CannotConnectException {
//        Connection conn=getConnection();
//        try {
//            PreparedStatement prepStat=conn.prepareStatement(insertStat);
//            prepStat.setString(1, key.toString());
//            prepStat.setBytes(2, getBytes(key));
//            prepStat.setBytes(3, getBytes(val));
//            prepStat.executeUpdate();
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace here
//            throw new CannotPersistException(t, "error adding new entry using creating Db connection and schema");
//        }
//    }// end of entering new row gen
//
//    /**
//     * Used to enter a completely new row in to the current table
//     * @param key
//     * @param val
//     * @exception CannotPersistException
//     */
//    private void addNewEntryOra(Serializable key, Serializable val) throws CannotPersistException, CannotConnectException {
//        Connection conn=getConnection();
//        try {
//            PreparedStatement prepStat=conn.prepareStatement(insertStat);
//            prepStat.setString(1, key.toString());
//            /*InputStream keyBin=*/getBinaryInputStream(key);
//            /*InputStream keyVal=*/getBinaryInputStream(val);
//            byte[] keyBytes=getBytes(key);
//            byte[] valBytes=getBytes(val);
//            prepStat.setBytes(2, keyBytes);
//            prepStat.setBytes(3, valBytes);
//            prepStat.executeBatch();
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace here
//            throw new CannotPersistException(t, "error adding new entry using creating Db connection and schema");
//        }
//    }// end of entering new row ora
//
//
//    /**
//     * Cache checking
//     * @param key
//     * @return boolean
//     */
//    private boolean entryExists(Serializable key) {
//        return list.contains(key.toString());
//    }
//
//
//    /**
//     * Conversion helper
//     * @param ser
//     * @return byte[]
//     */
//    private byte[] getBytes(Serializable ser) throws Exception {
//        ByteArrayOutputStream stream=new ByteArrayOutputStream();
//        ObjectOutputStream keyoos=new ObjectOutputStream(stream);
//        keyoos.writeObject(ser);
//        byte[] keyBytes=stream.toByteArray();
//        return keyBytes;
//    }// end of getBytes
//
//
//
//
//    /**
//     * ALL IMPL below is for INIT purposes
//     */
//
//    /**
//     * This method will be invoked by defauly by each persistence
//     * manager to read from a default location or one provided by
//     * the caller.
//     * @exception Exception
//     */
//    private void readProps(String filePath) throws Exception {
//        FileInputStream _stream=new FileInputStream(filePath);
//        props=new Properties();
//        props.load(_stream);
//
//        // using properties to set most used variables
//        driverName=props.getProperty("jdbc.Driver");
//        connStr=props.getProperty("jdbc.Conn").trim();
//        userName=props.getProperty("jdbc.User").trim();
//        userPass=props.getProperty("jdbc.Pass").trim();
//        createTable=props.getProperty("jdbc.table").trim();
//    }
//
//
//    /**
//     * Duplicate reader using stream instead of dile
//     * @param input
//     * @exception Exception
//     */
//    private void readProps(InputStream input) throws Exception {
//        props=new Properties();
//        props.load(input);
//
//        // using properties to set most used variables
//        driverName=props.getProperty("jdbc.Driver");
//        connStr=props.getProperty("jdbc.Conn");
//        userName=props.getProperty("jdbc.User");
//        userPass=props.getProperty("jdbc.Pass");
//        createTable=props.getProperty("jdbc.table");
//    }
//
//
//    /**
//     * Loads the driver using the driver class name. Drivers can be simply
//     * loaded by loading the class or by registering specifically using the
//     * JDBC DriverManager
//     * @exception Exception
//     */
//    private void loadDriver() throws Exception {
//        // driver classes when loaded load the driver into VM
//      ClassPathLoader.getLatest().forName(driverName);
//    }
//
//
//    /**
//     * Once the driver is loaded, the DB is ready to be connected. This
//     * method provides a handle to connect to the DB.
//     * @return Connection
//     * @exception CannotConnectException
//     */
//    private Connection getConnection() throws CannotConnectException {
//        try {
//            connStr=connStr.trim();
//            Connection conn=DriverManager.getConnection(connStr, userName, userPass);
//            if(log.isInfoEnabled()) log.info("userName=" + userName +
//                                             ", userPass=" + userPass + ", connStr=" + connStr);
//            return conn;
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            t.printStackTrace();
//            //trace here
//            throw new CannotConnectException(t, "Error in creating connection using provided properties ");
//        }
//    }// end of get conn..
//
//
//    /**
//     * Method is used for closing created connection.
//     * Pooling is not implemented currently, but will be made available
//     * as soon as this manager uses large number of transactions
//     * @param conn
//     */
//    private void closeConnection(Connection conn) {
//        try {
//            if(conn != null) {
//                conn.close();
//                conn=null;
//            }
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            //trace here
//            conn=null;
//        }
//    }// end of closeConn
//
//
//    /**
//     * Used to create table provided the DB instance
//     * @exception CannotCreateSchemaException
//     * @exception CannotConnectException
//     */
//    private void createDBTables() throws CannotCreateSchemaException, CannotConnectException {
//        Connection conn=this.getConnection();
//        Statement stat=null;
//        try {
//            stat=conn.createStatement();
//        }
//        catch(Exception e) {
//            //trace here..
//            e.printStackTrace();
//            throw new CannotConnectException(e, "there was an error in creating statements for persisting data using created connection");
//        }
//        try {
//            /*ResultSet set=*/stat.executeQuery("select * from replhashmap");
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            t.printStackTrace();
//            //use connection to create new statement
//            addSchemaToDB(conn);
//        }// end of out throwable..
//    }// end of method..
//
//
//    /**
//     * used to create required table within the DB
//     * @param conn
//     * @exception CannotCreateSchemaException
//     */
//    private void addSchemaToDB(Connection conn) throws CannotCreateSchemaException {
//        Statement stat=null;
////        Statement stat2=null;
//        try {
//
//            stat=conn.createStatement();
//            log.error(JGroupsStrings.DBPersistenceManager_EXECUTING_QUERY_FOR_ORACLE__0, createTable);
//// GemStoneAddition - this line commented out due to warnings from Veracode about possible SQL injection attacks
//            //            stat.executeQuery(createTable);
//            throw new UnsupportedOperationException("this functionality removed due to possibility of SQL injection attacks");
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//          SystemFailure.checkFailure(); // GemStoneAddition
//            t.printStackTrace();
//            // trace here
//            throw new CannotCreateSchemaException(t, "error was using schema with blobs");
//        }// end of catch
//
//                // clean up is required after init
//        finally {
//            try {
//                if(stat != null) stat.close();
//                this.closeConnection(conn);
//            }
//            catch (VirtualMachineError err) { // GemStoneAddition
//              SystemFailure.initiateFailure(err);
//              // If this ever returns, rethrow the error.  We're poisoned
//              // now, so don't let this thread continue.
//              throw err;
//            }
//            catch(Throwable t3) {
//              SystemFailure.checkFailure(); // GemStoneAddition
//            }
//        }// end of finally..
//    }// end of gen schema..
//
//    private Properties props=null;
//    private String driverName=null;
//    private String userName=null;
//    private String userPass=null;
//    private String connStr=null;
//    private String createTable=null;
////    private final boolean oracleDB=false;
//    private Vector list=null;
//
//
////    private static final String tabName="replhashmap";
//    private static final String insertStat="insert into replhashmap(key, keyBin, valBin) values  (?, ?, ?)";
//    private static final String updateStat="update replhashmap set keyBin = ?, valBin = ? where key like ?";
//    private static final String removeStat=" delete from replhashmap where key like ?";
////    private static final String createTableGen=" create table replhashmap(key varchar, keyBin varbinary, valBin varbinary)";
////    private static final String createTableOra=" create table replhashmap ( key varchar2(100), keyBin blob, valBin blob)";
}
