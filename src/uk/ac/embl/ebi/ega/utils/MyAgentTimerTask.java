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

package uk.ac.embl.ebi.ega.utils;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.TimerTask;
import uk.ac.embl.ebi.ega.downloadagent.EgaDownloadAgent;

/**
 *
 * @author asenf
 */
public class MyAgentTimerTask extends TimerTask {

    private EgaDownloadAgent api = null;
    
    public MyAgentTimerTask(EgaDownloadAgent api) {
        this.api = api;
    }
            
    @Override
    public void run() {
        System.out.println("Timer task execution (find new download requests).");
        
        System.gc();

        // Query local cache db for new download requests
        String[] listRestRequests = this.api.listRestRequests(null);
        System.out.println("Read " + (listRestRequests!=null?listRestRequests.length:"null") + " requests in local db.");

        HashSet<String> currentTickets = new HashSet<>();
        Enumeration<String> ticketeQueue = this.api.getTicketeQueue();
        
        // Add to queue of tickets to download
        if (listRestRequests != null && listRestRequests.length > 0) {
            for (int i=0; i<listRestRequests.length; i++) {
                StringTokenizer token = new StringTokenizer(listRestRequests[i], "\t");
                String ticket_ = token.nextToken();
                String label_ = token.nextToken();
                String name_ = token.nextToken();
                currentTickets.add(ticket_);
                
                if (!this.api.hasEntry(ticket_)) {
                    MyAgentDownloadEntry e = new MyAgentDownloadEntry(ticket_, label_, name_);

                    boolean putEntry = this.api.putEntry(e); // Put, if not yet present
                    if (putEntry)
                        System.out.println("Added Entry for: " + e.ticket + ", " + e.name);
                } 
            }
        }
        
        // Remove stale tickets from Agent queue
        while (ticketeQueue.hasMoreElements()) {
            String ticket = ticketeQueue.nextElement();
            if (!currentTickets.contains(ticket))
                this.api.removeEntry(ticket);
        }
    }    
}
