/*
 * Copyright 2014 EMBL-EBI.
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

import java.io.File;
import uk.ac.embl.ebi.ega.egadbapiwrapper.EgaDBAPIWrapper;
import uk.ac.embl.ebi.ega.utils.MyAgentDownloadEntry;

/**
 *
 * @author asenf
 */
public class EgaDownloadAgent_Thread extends Thread {

    private final String ticket;
    private final int index;

    private final MyAgentDownloadEntry ticketInfo;
    
    private String result;
    private boolean success = false;
    
    private EgaDBAPIWrapper api;
    
    private long starttime;
    
    public EgaDownloadAgent_Thread(String ticket, int index, EgaDBAPIWrapper api, MyAgentDownloadEntry ticketInfo) {
        this.ticket = ticket;
        this.index = index;
        this.ticketInfo = new MyAgentDownloadEntry(ticketInfo);
        this.api = api;
        this.starttime = System.currentTimeMillis();
        
        this.result = "";
    }
    
    @Override
    public void run() {
        String down_name_ = this.ticketInfo.name;
        if (down_name_ != null && down_name_.endsWith("gpg")) {
            if (!this.ticket.contains("?org="))
                down_name_ = down_name_.substring(0, down_name_.length()-3) + "cip";
        }
        if (down_name_ != null)
            down_name_ = down_name_.replaceAll("/", "_");
        
        String prefix = this.ticketInfo.label + "/";
        down_name_ = prefix + down_name_;
        
        System.out.println("Starting download: " + this.ticketInfo.name + ": " + down_name_);
        
        long time = System.currentTimeMillis();
        String[] ds = this.api.download(ticket, down_name_, prefix, true);
        if (ds!=null && (ds[0].equalsIgnoreCase("Error establishing Connection!!") || ds[0].equalsIgnoreCase("Unable to establish download stream"))) {
            this.result = "Download Failed: " + this.ticketInfo.name + "  " + this.ticket;
            this.result += "   SKIP";
            return;
        }
        time = System.currentTimeMillis() - time;
        long length = 0;
        if (ds!=null && ds.length > 0) {
            if (ds[0].equalsIgnoreCase("Ticket is Locked!"))
                length = -10;
            else
                length = (new File(ds[0])).length();
        }
        
        this.success = (length > 0 || (ds!=null && ds.length>1 && ds[1].equalsIgnoreCase("Success")));
        if (success) {
            double rate = (length * 1.0 / 1024.0 / 1024.0) / (time * 1.0 / 1000.0);

            StringBuilder sb = new StringBuilder();
            try {
                sb.append("Completed Download: ").append(this.ticketInfo.name);
                for (int i=0; i<ds.length; i++)
                    sb.append("  ").append(ds[i].substring(ds[i].lastIndexOf("/")+1)).append(" ");
                sb.append("Rate: ").append(rate).append(" MB/s");
            } catch (Throwable t) {
                sb.append("Error requesting ").append(this.ticketInfo.name).append(" (").append(this.ticket).append(")");
            }
            this.result = sb.toString();
        } else {
            this.result = "Download Failed: " + this.ticketInfo.name + "  " + this.ticket;
            if ( (length==-10) || ((ds!=null && ds.length > 2 && ds[2].equalsIgnoreCase("true")))) {
                System.out.println("Skipping this ticket!");
                this.result += "   SKIP";
            }
        }
    }
    
    public int getIndex() {
        return this.index;
    }

    public String getResult() {
        return this.result;
    }
    
    public boolean getSuccess() {
        return this.success;
    }
    
    public String getTicket() {
        return this.ticket;
    }
    
    public MyAgentDownloadEntry getDownInfo() {
        return this.ticketInfo;
    }
    
    public long getStartTime() {
        return this.starttime;
    }
}
