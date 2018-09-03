/*
 * Copyright 2015 EMBL-EBI.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.embl.ebi.ega.egadbapiwrapper;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import oodforsqljet.database.Table;
import static oodforsqljet.database.Table.getTableInstance;
import oodforsqljet.database.exceptions.DatabaseException;
import uk.ac.embl.ebi.ega.egaapiwrapper.EgaAPIWrapper;
import uk.ac.embl.ebi.ega.sqljetdb.MyPendingRequestTable;
import uk.ac.embl.ebi.ega.utils.EgaFile;
import uk.ac.embl.ebi.ega.utils.EgaTicket;
import uk.ac.embl.ebi.ega.utils.MyTableConnectionThread;

/**
 *
 * @author asenf
 * 
 * Transparent Local-DB layer for APIWrapper
 * Purpose: locally keep track of any pending files when datasets are requested
 *  and allow for the pending files to be requested/downloaded when they become
 *  available.
 * Also - experimental feature: download agent to run permanently in the background
 */
public class EgaDBAPIWrapper {

    private EgaAPIWrapper api = null;   // API Wrapper Object
    private boolean db = false;         // DB Present?
    private volatile boolean update = false; // Update in Progress?    
    private String user = "";
    private boolean connected = false;
    
    public EgaDBAPIWrapper(String infoServer, String dataServer, boolean ssl,
                     String globusServer, String globusPrefix) {
        this.api = new EgaAPIWrapper(infoServer, dataServer, ssl,
                    globusServer, globusPrefix);
    }
    
    public EgaDBAPIWrapper(String infoServer, String dataServer, boolean ssl) {
        this.api = new EgaAPIWrapper(infoServer, dataServer, ssl);
    }
    
    public EgaDBAPIWrapper(String username, char[] password, 
            String infoServer, String dataServer) {
        this.api = new EgaAPIWrapper(username, password, infoServer, dataServer);
    }    
    
    public void setBackupDataServer(String dataServer) {
        this.api.setBackupDataServer(dataServer);
    }
    
    public void setGlobusServer(String globusServer) {
        this.api.setGlobusServer(globusServer);
    }
    
    public boolean hasDB() {
        return this.db;
    }

    public void setDB(boolean value) {
        this.db = value;
    }
    
    public void setConnected(boolean value) {
        this.connected = value;
    }

    public void setUpdate(boolean val) {
        this.update = val;
    }
    
    public boolean isUpdate() {
        if (this.db)
            return this.update;
        else
            return false;
    }
    
    public String myIP() {
        return this.api.myIP().trim();
    }

    public void setAlt(boolean value) {
        this.api.setAlt(value);
    }
    
    public boolean getAlt() {
        return this.getAlt();
    }
    
    public boolean session() {
        return this.api.session();
    }
    
    public void logout() {
        this.api.logout();
    }
    
    public void logout(boolean verbose) {
        this.api.logout(verbose);
    }
    
    // Connect the object to a local SQLIte DB file - files are user-specific and plain-text
    public boolean connectDB(String username) {
        this.user = username;
        
        String db_name = dbPath();
        try {System.out.println("Using Cache DB at: " + (new File(db_name)).getCanonicalPath());} catch (Throwable th) {
            System.err.println(th.toString());
        }
        
        // Connect to DB in a separate thread. Wait for 5 seconds to complete; otherwise
        // move on. The thread will update the object status when it is ready
        Thread x = new Thread(new MyTableConnectionThread(this, db_name));
        x.start();
        
        long time = System.currentTimeMillis();
        while ( ((System.currentTimeMillis())-time) < 5000 && x.isAlive() ) {
            try {Thread.sleep(950);} catch (InterruptedException ex) {
                System.err.println(ex.toString());
            }
        }
        
        //try {
        //    Table.associatePackageToSqlJetDatabase(MyPendingRequestTable.class.getPackage(),
        //                                            new SqlJetWrapper(
        //                                            new File(db_name), false));
        //    this.db = true;
        //} catch (DatabaseException ex) {
        //    System.out.println("Cache Access error: " + ex.getLocalizedMessage());
        //}
        //this.connected = this.db;
        
        return this.db; // may not be the final answer...
    }
    
    // Logging in.
    // - Update Pending Request Files
    public boolean login(String username, char[] password) {
        return login(username, password, this.db);
    }
    public boolean login(String username, char[] password, boolean useDB) {
        boolean result = this.api.login(username, password);
        if (result) result = (this.api.getUser() != null) && !this.api.getUser().equalsIgnoreCase("null");
        if (!result) {
            this.api.logout(false);
            return result;
        }

        if (useDB) {
            connectDB(this.api.getUser());
            if (this.db) {
                System.out.println("Updating Cache DB.");
                setUpdate(true);
                updateDatabase();
                setUpdate(false);
            }
        }
        
        return result;
    }
    
    public String getLoginMessage() {
        return this.api.getLoginMessage();
    }
    
    // Globus-specific API functions - log in to Globus, Start Globus-Staged transfer
    public boolean globusLogin(String username) {
        return this.api.globusLogin(username);
    }
    public boolean globusLogin(String username, char[] password) {
        return this.api.globusLogin(username, password);
    }

    public String getGlobusMessage() {
        return this.api.getGlobusMessage();
    }
    
    public String globusStartTransfer(String request, String endpoint) {
        return this.api.globusStartTransfer(request, endpoint);
    }
    
    // Called from Timer Task - local DB is updated in the background periodically!
    public void updateDatabase() {
        boolean error = false;
        if (this.db) {
            if (!this.connected) connectDB(this.api.getUser());
            
            // Update Pending Requests ---------------- (prerequisit: request must have been made using this client!) -- TESTS PENDING
            HashMap<String, Integer> rd = new HashMap<>();
            try {
                MyPendingRequestTable mpt=(MyPendingRequestTable)getTableInstance(MyPendingRequestTable.class);
                
                // Step 1: Check all pending request files, to see ifthere has been any update to the status
                HashMap<String, EgaFile[]> dsfiles = new HashMap<>(); // Dataset -> File
                
                ArrayList<MyPendingRequestTable.Record> pendingrecs = mpt.getRecords();
                for (MyPendingRequestTable.Record r : pendingrecs) {
                    
                    String dataset = r.dataset_stable_id;
                    if (!dsfiles.containsKey(dataset)) {
                        EgaFile[] listDatasetFiles = this.api.listDatasetFiles(dataset);
                        dsfiles.put(dataset, listDatasetFiles);
                    }
                    
                    EgaFile[] dsf = dsfiles.get(dataset);
                    String fileid = r.file_stable_id;
                    for (EgaFile f:dsf) {
                        if (f.getFileID().equalsIgnoreCase(fileid)) {
                            if (f.getStatus().equalsIgnoreCase("available")) {
                                String request = r.label;
                                if (!rd.containsKey(request))
                                    rd.put(request, 1);
                                else
                                    rd.put(request, (rd.get(request)+1));
                            }
                            break;
                        }
                    }
                    
                }                
            } catch (DatabaseException ex) {
                System.err.println("DB Error (Pending Requests): " + ex.getLocalizedMessage());
                error = true;
            }
            
            // Done. Print result.
            if (!error && !rd.isEmpty()) {
                System.out.println(rd.size() + " requests have updated files:");
                Iterator<String> iter = rd.keySet().iterator();
                while (iter.hasNext()) {
                    String request = iter.next();
                    System.out.println("\tRequest: " + request + " has " + rd.get(request) + " files no longer pending.");
                }
                System.out.println();
            }
        }
        
        if (error)
            rebuildDB();
    }
    
    // Close DB, delete DB file, re-build new DB -- in case there is an error
    private void rebuildDB() {
        System.out.println("Re-Building loal DB, because of error condition! Pending file information will be lost!");
        String path = dbPath();
        closeDB();
        try { (new File(path)).delete(); } catch (Throwable th) {System.err.println(th.getLocalizedMessage());}
        connectDB(this.api.getUser());
        updateDatabase();
    }
    
    // TODO - update code...
    // Find a suitable location for the local SQLite DB file
    private String dbPath() {
        String the_user = System.getProperty("user.name");
        String db_name = the_user + "_" + this.user + "_ega_db.sqlite";
        String db_name_local = db_name;
        
        // Set up database location (for Windows, Linux, Mac)
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("win") >= 0) {
            db_name = "/ProgramData/EgaDownload/" + db_name;
        } else if (os.indexOf("mac") >= 0) {
            db_name = "/usr/local/var/egadownload/" + db_name;
        } else if (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") > 0 ) {
            db_name = "/var/lib/egadownload/" + db_name;
        } else if (os.indexOf("sunos") >= 0) {
            // Nothing special, yet.
        }
        
        // Create path, of necessary; or revert to local path; print DB location
        File f = new File(db_name);
        File parentFile = f.getParentFile();
        if (parentFile!=null && !f.exists()) {
            boolean mkdirs = parentFile.mkdirs();
            if (!mkdirs ) { 
                db_name = db_name_local;
            }
        }
        
        // Return path, or loal DB name, if no write access
        try {if (!f.exists()) f.createNewFile();} catch (Throwable th) {
            System.err.println(th.toString());
        }
        if (f.exists() && f.canRead())
            return f.getAbsolutePath();
        else
            return db_name = db_name_local;
    }
    
    // -------------------------------------------------------------------------
    
    // "acquire" a request from a different IP address
/*    
    public String[] localize(String descriptor) {
        String[] result = this.api.localize(descriptor);
        return result;
    }
*/    
    public boolean setSetPath(String path) {
        return this.api.setSetPath(path);
    }
    
    public String getPath() {
        return this.api.getPath();
    }
    
    public void setVerbose(boolean value) {
        this.api.setVerbose(value);
    }    

    public boolean getVerbose() {
        return this.api.getVerbose();
    }
    
    public void setUdt(boolean value) {
        this.api.setUdt(value);
    }    
    public boolean getUdt() {
        return this.api.getUdt();
    }
    
    public String getInfoServer() {
        return this.api.getInfoServer();
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    
    public String[] listDatasets() {
        String[] result = this.api.listDatasets();
        if (result!=null)
            Arrays.sort(result);       
        return result;
    }

    public EgaFile[] listDatasetFiles(String dataset) {
        EgaFile[] result = this.api.listDatasetFiles(dataset);
        return result;
    }    
    
    public EgaFile[] listFileInfo(String fileid) {
        EgaFile[] result = this.api.listFileInfo(fileid);
        return result;
    }    
    
    public EgaTicket[] listRequests() {
        return listRequest("");
    }
    
    public EgaTicket[] listRequest(String descriptor) {
        EgaTicket[] result = this.api.listRequest(descriptor);
        if (result==null)
            result = new EgaTicket[]{};
        
        // See if there are any pending requests
        ArrayList<EgaTicket> pending = new ArrayList<>();
        if (this.db) { // Value-added: Pending Requests
            EgaTicket[] listPendingRequests = listPendingRequests(descriptor);
            for (EgaTicket f:listPendingRequests)
                pending.add(f);
        }
        
        EgaTicket[] result_ = null;
        if (pending.size() > 0) {
            result_ = new EgaTicket[result.length + pending.size()];
            System.arraycopy(result, 0, result_, 0, result.length);
            for (int i=0; i<pending.size(); i++)
                result_[result.length+i] = pending.get(i);
        } else if (result.length > 0) {
            result_ = new EgaTicket[result.length];
            System.arraycopy(result, 0, result_, 0, result.length);
        }
        
        return result_;
    }
    
    public EgaTicket[] listPendingRequests(String descriptor) {
        EgaTicket[] result = null;
        
        ArrayList<EgaTicket> pending = new ArrayList<>();
        if (this.db) { // Value-added: Pending Requests
            try {
                MyPendingRequestTable mpt = (MyPendingRequestTable)getTableInstance(MyPendingRequestTable.class);
                ArrayList<MyPendingRequestTable.Record> penrecs = (descriptor==null||descriptor.length()==0)?mpt.getRecords():mpt.getPendingFileRecords(descriptor);
                
                if (penrecs!=null && !penrecs.isEmpty()) { // Already Cached
                    for (int i=0; i<penrecs.size(); i++) {
                        if (penrecs.get(i).label.equalsIgnoreCase(descriptor) || descriptor.length()==0) {
                            EgaTicket pF = new EgaTicket();
                            pF.setLabel(penrecs.get(i).label);
                            pF.setFileID(penrecs.get(i).file_stable_id);
                            pF.setFileName(penrecs.get(i).file_name);
                            
                            pending.add(pF);
                        }
                    }
                }                
            } catch (DatabaseException ex) {
                System.err.println(ex.toString());
            }
        }
        
        result = new EgaTicket[pending.size()];
        for (int i=0; i<pending.size(); i++)
            result[i] = pending.get(i);

        return result;
    }
    
    public EgaTicket[] listAllRequests() {
        EgaTicket[] result = this.api.listAllRequests();
        if (result==null)
            result = new EgaTicket[]{};
        
        // See if there are any pending requests
        ArrayList<EgaTicket> pending = new ArrayList<>();
        if (this.db) { // Value-added: Pending Requests
            EgaTicket[] listPendingRequests = listPendingRequests("");
            for (EgaTicket f:listPendingRequests)
                pending.add(f);
        }

        EgaTicket[] result_ = null;
        if (pending.size() > 0) {
            result_ = new EgaTicket[result.length + pending.size()];
            System.arraycopy(result, 0, result_, 0, result.length);
            for (int i=0; i<pending.size(); i++)
                result_[result.length+i] = pending.get(i);
        } else if (result.length > 0) {
            result_ = new EgaTicket[result.length];
            System.arraycopy(result, 0, result_, 0, result.length);
        }

        return result_;
    }
    public String[] listAllRequestsLight() {
        String[] result = this.api.listAllRequestsLight();
        
        // Skip pending in this implementation
        
        return result;
    }    
    public EgaTicket[] listTicketDetails(String ticket) {
        EgaTicket[] result = this.api.listTicketDetails(ticket);
        
        return result;
    }    
    
    public String[] requestByID(String id, String type, String reKey, String descriptor) {
        return requestByID(id, type, reKey, descriptor, "");
    }    
    
    public String[] requestByID(String id, String type, String reKey, String descriptor, String target) {

        // Cache Dataset specified in Request locally --
        int files_in_ds = 0;
        EgaFile[] datasetFiles = null;

        // Request the dataset, via REST Call
        String[] result = this.api.requestByID(id, type, reKey, descriptor, target);
        
        // Store request tickets in local db && Deal with Pending files
        if (this.db) {
            String ip = this.api.myIP();
            try {
                EgaFile[] listDatasetFiles = null; //this.api.listDatasetFiles(id);
                MyPendingRequestTable mpt=(MyPendingRequestTable)getTableInstance(MyPendingRequestTable.class);
                
                if (type.equalsIgnoreCase("dataset")) {
                    listDatasetFiles = this.api.listDatasetFiles(id); // All files in dataset
                    
                    // Deal with Pending files (if a dataset was requested)
                    if (listDatasetFiles != null && listDatasetFiles.length > 0) {
                        for (EgaFile f : listDatasetFiles) {
                            if (f.getStatus().equalsIgnoreCase("pending")) {
                                mpt.addRecord(descriptor, f.getFileSize(), f.getFileID(), f.getFileName(), id);                            
                                files_in_ds++;
                            }
                        }
                    }
                } else if (type.equalsIgnoreCase("file")) {
                    listDatasetFiles = this.api.listFileInfo(ip); // Just the specified file
                    
                    // Deal with Pending files (if a dataset was requested)
                    for (EgaFile f : listDatasetFiles) {
                        if (f.getFileID().equalsIgnoreCase(id)) {
                            if (f.getStatus().equalsIgnoreCase("pending")) {
                                mpt.addRecord(descriptor, f.getFileSize(), f.getFileID(), f.getFileName(), id);                            
                                files_in_ds++;
                            }
                            break;
                        }
                    }
                }
            } catch (DatabaseException ex) {
                System.err.println(ex.toString());
            }
            
            if (files_in_ds > 0 && result!=null && files_in_ds>result.length)
                System.out.println("This request contains " + files_in_ds + " Pending files!");
                System.out.println("\t(Pending files can not be downloaded, because they are not yet in the archive.)");
        }        
        
        if (result!=null)
            Arrays.sort(result);       
        return result;
    }    

    public String[] requestPending(String descriptor, String reKey) {
        String[] result = null;

        ArrayList<String> requesttickets = new ArrayList<>();
        if (this.db) {
            // Get Pending files for given dataset
            try {
                MyPendingRequestTable mpt=(MyPendingRequestTable)getTableInstance(MyPendingRequestTable.class);
                ArrayList<String> pendingFiles = mpt.getPendingFiles(descriptor); // File IDs
            
                // Request files via API
                int cnt = 0;
                for (int i=0; i<pendingFiles.size(); i++) {
                    String[] requestByID = this.requestByID(pendingFiles.get(i), "file", reKey, descriptor);
                    if (requestByID!=null && requestByID.length>0 && !requestByID[0].equalsIgnoreCase("0")) {
                        for (int j=0; j<requestByID.length; j++)
                            requesttickets.add(requestByID[j]);
                        cnt++;
                    }
                }
                if (cnt==0) 
                    System.out.println("No new files available for this request.");
                else
                    System.out.println(cnt + " pending files were added to request: " + descriptor);
            } catch (DatabaseException ex) {
                System.err.println(ex.toString());
            }
        } else {
            System.out.println("No new files available for this request.");
        }

        result = new String[requesttickets.size()];
        for (int i=0; i<requesttickets.size(); i++)
            result[i] = requesttickets.get(i);
        if (result!=null)
            Arrays.sort(result);       
        return result;
    }

    public String[] downoad_metadata(String dataset) {
        return this.api.download_metadata(dataset);
    }
    
    public String[] download(String ticket, final String down_name, String org) {
        return download(ticket, down_name, org, false);
    }
    public String[] download(String ticket, final String down_name, String org, boolean agent) {
        String[] result = (agent || this.api.getAlt())?
                this.api.download_tcp_url(ticket, down_name, org):
                this.api.download_netty(ticket, down_name, org);
        
        return result;
    }
    
    public String[] delete_request(String request) {
        String[] result = this.api.delete_request(request);
        // Delete pending files for this request, if present
        if (this.db)
            delete_pending_request(request);
        
        return result;
    }    
    
    public String[] delete_ticket(String request, String ticket) {
        String[] result = this.api.delete_ticket(request, ticket);
        
        return result;
    }    
    
    public void delete_pending_request(String descriptor) {
        if (this.db) {
            try {
                MyPendingRequestTable mpt=(MyPendingRequestTable)getTableInstance(MyPendingRequestTable.class);
                mpt.removeRequest(descriptor);
            } catch (DatabaseException ex) {
                System.err.println(ex.toString());
            }
        }
    }    

    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    
    public void decrypt(String key, String destination, List<String> files, int pwb) {
        this.api.decrypt(key, destination, files, pwb);
    }    

    public void decrypt(String key, String destination, List<String> files, int pwb, boolean delete) {
        this.api.decrypt(key, destination, files, pwb, delete);
    }    
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    public void closeDB() {
        try {
            Table.closeDatabase(MyPendingRequestTable.class.getPackage());
        } catch (DatabaseException ex) {
            System.err.println("Error closing local db: " + ex.getLocalizedMessage());
        }
    }
}
