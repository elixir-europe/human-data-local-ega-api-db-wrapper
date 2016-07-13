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
package uk.ac.embl.ebi.ega.downloadagent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import static oodforsqljet.database.Table.getTableInstance;
import oodforsqljet.database.exceptions.DatabaseException;
import uk.ac.embl.ebi.ega.egadbapiwrapper.EgaDBAPIWrapper;
import uk.ac.embl.ebi.ega.sqljetdb.MyRequestTable;
import uk.ac.embl.ebi.ega.utils.EgaTicket;
import uk.ac.embl.ebi.ega.utils.MyAgentDownloadEntry;
import uk.ac.embl.ebi.ega.utils.MyAgentTimerTask;

/**
 *
 * @author asenf
 * 
 * Reads local cache db - downloads requested files
 * Files will be stored in subdirectories based on label
 * 
 * 
 */
public class EgaDownloadAgent {
    
    private String username = null;
    private String dest_path = "";
    private ConcurrentHashMap<String, MyAgentDownloadEntry> queue = null;
    private HashSet<String> completed = null;

    private static Timer theTimer;
    public static boolean shutdown = false, shutdown_complete = false;
    
    private boolean udt = false; // Default TCP
    private int threads = 8;     // Default 8 Threads

    private String ip = "";
    
    private static boolean isCRG = false;
    
    // ----------------------------
    private EgaDBAPIWrapper api;
    
    
    public EgaDownloadAgent(String username, String dest_path, int threads, boolean udt, String server, int port, String org) {
        System.out.println("Creating download object.");
        this.username = username;
        
        String dataServer = server + ":" + port;
        this.api = new EgaDBAPIWrapper("ega.ebi.ac.uk", dataServer, true);
        boolean connectDB = this.api.connectDB(username);
        
        if (!connectDB) {
            System.out.println("Can't file local SQLite Caching DB!");
            return;
        }
        
        // Update Request Table in local DB with all requests
        EgaTicket[] listAllRequests = this.api.listAllRequests();
        try {
            MyRequestTable mpt=(MyRequestTable)getTableInstance(MyRequestTable.class);
            
            // Potentially -- empty out existing table?
            
            for (int i=0; i<listAllRequests.length; i++) {
                mpt.addRecord(listAllRequests[i].getLabel(), 
                              0L, 
                              listAllRequests[i].getTicket(), 
                              listAllRequests[i].getFileID(), 
                              listAllRequests[i].getFileName(), 
                              dest_path, 
                              this.api.myIP());
            }
        } catch (DatabaseException ex) {
            System.out.println("DB Error (Pending Requests): " + ex.getLocalizedMessage());
        }

        this.api.setSetPath(dest_path);
        this.api.setUdt(udt);
        //this.api.setVerbose(true);
        
        if (org!=null && org.equalsIgnoreCase("crg"))
            isCRG = true;
        
        this.queue = new ConcurrentHashMap<>();
        this.completed = new HashSet<>();
        this.threads = threads <=0?8:threads;
        if (isCRG) this.threads = 16;
        
        System.out.println("Download Threads: " + this.threads);
        
        this.ip = this.api.myIP();
        
        //System.out.println("Creating timer task."); // place requests in Queue
        System.out.println("Getting files to download."); // place requests in Queue
        TimerTask timerTask = new MyAgentTimerTask(this);
        timerTask.run();
        //theTimer = new Timer(true);
        //theTimer.scheduleAtFixedRate(timerTask, 0, 900000); // immediately, then every 15 min
        
        System.out.println("Object Setup Complete.");
    }
    
    public void run() {
        System.out.println("Staring Download loop now!");
        
        ArrayList<ArrayList<String>> batchQueue = new ArrayList<>();
        
        // Run algorithm
        while (!shutdown) { // While shutdown is not initiated, run the loop
            // Select up to n tickets from queue, via iterator
            if (this.queue != null && !this.queue.isEmpty()) {
                Enumeration<String> keys = this.queue.keys();

                while (keys.hasMoreElements()) {
                    int cnt = 0;
                    ArrayList<String> oneBatch = new ArrayList<>();
                    while (keys.hasMoreElements()) { // && cnt++ < 60) {
                        String ticket = keys.nextElement();
                        if (!this.completed.contains(ticket))
                            oneBatch.add(ticket);
                    }
                    batchQueue.add(oneBatch);                    
                }
                                
                // Download each batch in turn; then repeat
                for (ArrayList<String> element:batchQueue) {
                    String[] tickets = element.toArray(new String[element.size()]);

                    // Download the selected tickets -- timed!
                    try {
                        runDownPar(tickets, this.threads);
                    } catch (IOException ex) {}
                    
                    try {Thread.sleep(15000);} catch (InterruptedException ex) {}
                }
                
            } else {
                System.out.println("Download Queue currently empty");
            }
            
            // Clear out old data structures
            Iterator<String> iter = this.completed.iterator();
            while (iter.hasNext()) {
                String ticket = iter.next();
                if (this.queue.containsKey(ticket)) {
                    this.queue.remove(ticket);
                    deleteTicket(ticket);
                }
            }
            this.completed.clear();
            try {Thread.sleep(60000);} catch (InterruptedException ex) {}            
        }

        // If shutdown is selected, once current downloads are complete indicated readiness for process to end
        //theTimer.cancel();
        shutdown_complete = true;
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    
    private void runDownPar(String[] tickets, int numThreads) throws IOException {

        // Step 1: Set up all tickets (i.e. get name of files to be downloaded
        EgaDownloadAgent_Thread my_threads[] = new EgaDownloadAgent_Thread[numThreads];
        int count = 0, savecount = 0, size = tickets.length;
System.out.println("Tickets: " + tickets.length);
        
        boolean alive = true;
        long allalivetime = 0;
        do { // perform test for all selected files
            ArrayList indices = new ArrayList(); // indices of "free" threads
            alive = false;
            for (int i=0; i<numThreads; i++) { // find threads that have ended
                if ( (my_threads[i] != null) && (my_threads[i].isAlive()) ) {
                    long delta = System.currentTimeMillis() - my_threads[i].getStartTime();
                    if (delta > 7200000) { // If the thread ran over two hours, stop it
                        String _ticket = my_threads[i].getTicket();
                        System.out.println("Download of ticket " + _ticket + " appears to have stalled ("+delta+" ms). Stopping download!");
                        my_threads[i].stop();
                        my_threads[i] = null;
                        savecount++; // count completed threads
                        System.out.println("Cancelled Downloading File " + savecount + " of " + (size));
                        deleteTicket(_ticket); // Remove from local DB                            
                        indices.add(i); // 'free' thread slot
                    } else 
                        alive = true;
                } else {
                    allalivetime = System.currentTimeMillis();
                    if (my_threads[i] != null) { // indicates completed Thread - post process
                        String _ticket = my_threads[i].getTicket();
                        MyAgentDownloadEntry _info = my_threads[i].getDownInfo();
                        if (my_threads[i].getSuccess()) { // If the download succeeded
                            System.out.println(my_threads[i].getResult());
                            my_threads[i] = null; // free up thread slot
                            savecount++; // count completed threads
                            System.out.println("Completed Downloading File " + savecount + " of " + (size));
                            this.queue.remove(_ticket); // Done - remove from queue
                            this.completed.add(_ticket);
                            deleteTicket(_ticket); // Remove from local DB                            
                            
                            System.out.println("--> Ticket " + _ticket + " removed from queue " + (!this.queue.containsKey(_ticket)));
                            indices.add(i); // 'free' thread slot
                        } // Don't re-try immediately - move on!
                    } else
                        indices.add(i); // 'free' thread slot
                }
            }

            // Start over if stalled
            long delta = System.currentTimeMillis()-allalivetime;
            if (delta > 36000000 && indices.size()==0) { // If nothing happened for 10 hours - start over
                System.out.println("Starting over, canceling all downloads! Stalled for " + delta + "ms!");
                for (int i=0; i<numThreads; i++) { // find threads that have ended
                    if (my_threads[i]!=null && my_threads[i].isAlive()) my_threads[i].stop();
                    try {Thread.sleep(500);} catch (InterruptedException ex) {}
                    my_threads[i] = null;
                    indices.add(i);
                }
            }
            
            // Previous loop determined free threads; fill them in the next loop
            if (indices.size() > 0 && count < size) { // If there are open threads, then
                for (int i=0; i<indices.size(); i++) { // Fill all open spaces
                    if (count < size) { // Catch errors

                        // Index [0->numThreads-1] of this thread
                        int index = Integer.parseInt(indices.get(i).toString());
                        
                        // Instantiate download thread object for ticket:count [0->numTickets-1]
                        String ticket = tickets[count];
                        
                        my_threads[index] = null;
                        if (this.queue.containsKey(ticket) && !this.completed.contains(ticket)) { // Skip stale tickets
                            if (isCRG)
                                ticket = ticket + "?org=crg"; // Indicate mirror download to an organization

                            my_threads[index] = new EgaDownloadAgent_Thread(ticket, index, this.api, this.queue.get(tickets[count]));
                        }
                            
                        boolean print = true;
                        if (my_threads[index] != null) {
                            my_threads[index].start(); // Start the download thread
                        } else {
                            System.out.println("Skipping: " + tickets[count]);
                            savecount++; // skip; count as 'completed' thread, so that process can complete
                            print = false;
                        }
                        count++; // count started threads
                        if (print) System.out.println("Started Downloading File " + count + " of " + (size));
                    }
                }
            }

            // runs until the number of completed threads equals the number of files, and all threads completed (redundant)
        }  while ((savecount < size) || alive);

        System.out.println("Batch Processed.");
    }
    
    // Own DB Interaction functions --------------------------------------------
    
    public void deleteTicket(String ticket) {
        try {
            MyRequestTable mrt=(MyRequestTable)getTableInstance(MyRequestTable.class);
            mrt.removeRecord(ticket);
            
            ArrayList<MyRequestTable.Record> records = mrt.getRecords(ticket);
            if (records==null || records.size()==0)
                System.out.println("Remove Success: " + ticket);
        } catch (DatabaseException ex) {}            
    }    

    public String[] listRestRequests(String descriptor) {
        String[] result = null;
        
        System.out.println("Reading requests from local DB: " + descriptor);
        try {
            MyRequestTable mrt = (MyRequestTable)getTableInstance(MyRequestTable.class);
            System.out.println("getting Table mrt: " + (mrt!=null) + "  IP = " + ip);
            ArrayList<MyRequestTable.Record> rerecs = (descriptor==null||descriptor.length()==0)?mrt.getRecordsByIp(ip):mrt.getRecordsByLabel(descriptor, ip);
            

            if (rerecs!=null && !rerecs.isEmpty()) { // Already Cached
                result = new String[rerecs.size()];

                for (int i=0; i<rerecs.size(); i++)
                    result[i] = rerecs.get(i).ticket + "\t" + rerecs.get(i).label + "\t" + rerecs.get(i).path;

            }                
        } catch (DatabaseException ex) {
            System.out.println("DB Error: " + ex.getLocalizedMessage());
        }
        
        return result;
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    public boolean putEntry(MyAgentDownloadEntry entry) {
        boolean result = false;
        if (entry!=null && !this.queue.containsKey(entry.ticket) && entry.name.length()>0) { // Skip requests without filenames
            if (!this.completed.contains(entry.ticket)) {
                this.queue.put(entry.ticket, entry);
            }
            result = true;
        }
        return result;
    }
    
    public boolean removeEntry(String entry) { // ticket
        boolean result = false;
        if (entry!=null && !this.queue.containsKey(entry)) { // Skip requests without filenames
            this.queue.remove(entry);
            result = true;
        }
        return result;
    }
    
    public Enumeration<String> getTicketeQueue() {
        Enumeration<String> keys = this.queue.keys();
        return keys;
    }
    
    public boolean hasEntry(String ticket) {
        return this.queue.containsKey(ticket);
    }
    
    public boolean isUDT() {
        return this.udt;
    }
    
    public String getDestPath() {
        return this.dest_path;
    }
    
    public static boolean isCRG() {
        return isCRG;
    }
}
