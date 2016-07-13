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

package uk.ac.embl.ebi.ega.direct;

import uk.ac.embl.ebi.ega.egadbapiwrapper.EgaDBAPIWrapper;
import uk.ac.embl.ebi.ega.utils.EgaFile;
import uk.ac.embl.ebi.ega.utils.EgaTicket;

/**
 *
 * @author asenf
 */
public class testMeDB {
    private EgaDBAPIWrapper api;
    private boolean login = false;
    
    public testMeDB(String username, String password) {
        
        this.api = new EgaDBAPIWrapper("pg-ega-pro-05.ebi.ac.uk:8101", "ega.ebi.ac.uk", false);
        login = this.api.login(username, password.toCharArray());
        System.out.println("Login Success? " + login);
        
    }
    
    private void test() {
        if (!login)
            return;
        
        System.out.println("Commencing Tests");
        
        // Test 1: Get Dataset - don't print
        System.out.println("Listing Datasets");
        String[] listDatasets = this.api.listDatasets();
        System.out.println(listDatasets.length + " datasets retrieved.");

        // Test 2: Get Files for Datasets - print first five files
        System.out.println("\nListing Files:\n");
        System.out.println("Listing Dataset Files");
        String dataset_with_pending = ""; // used for a later test
        String file_for_request = ""; // used for a later test
        if (listDatasets != null) {
            for (String dataset : listDatasets) {
                // Print first 5 files
                EgaFile[] listDatasetFiles = this.api.listDatasetFiles(dataset);
                if (listDatasetFiles!=null && listDatasetFiles.length>0) {
                    System.out.println("Dataset: " + dataset);
                    for (int j=0; j<listDatasetFiles.length; j++) {
                        if (j<5)
                            System.out.println("   " + j + ": " + listDatasetFiles[j].getFileName() + " :: " + listDatasetFiles[j].getFileSize() + " :: " + listDatasetFiles[j].getStatus());
                        if (file_for_request.length() == 0)
                            file_for_request = listDatasetFiles[j].getFileID();
                        if (dataset_with_pending.length() == 0 && listDatasetFiles[j].getStatus().equalsIgnoreCase("pending"))
                            dataset_with_pending = dataset;
                    }
                }
            }
        }
        
        // Test 3: Request a Dataset wih Pending files
        System.out.println("\nRequesting one dataset: " + dataset_with_pending + "\n");
        String[] requestDatasetByID = this.api.requestByID(dataset_with_pending, "dataset", "abc", "pendingTest");
        if (requestDatasetByID!=null && requestDatasetByID.length > 0)
            System.out.println(requestDatasetByID[0] + " files requested.");

        // Test 4: Request a File
        System.out.println("\nRequesting one file: " + file_for_request + "\n");
        String[] requestFiletByID = this.api.requestByID(file_for_request, "file", "abc", "pendingTest");
        if (requestFiletByID!=null && requestFiletByID.length > 0)
            System.out.println(requestFiletByID[0] + " files requested.");
        
        // Test 5: List all Requests
        System.out.println("\nListing All Requests:\n");
        EgaTicket[] listAllRequests = this.api.listAllRequests();
        if (listAllRequests!=null) {
            for (int i=0; i<listAllRequests.length; i++)
                System.out.println(" :all: " + listAllRequests[i].getLabel() + "  " + listAllRequests[i].getTicket());
        }
        
        // Test 6: List one Requests
        System.out.println("\nListing One Request:\n");
        String ticket = "";
        EgaTicket[] listRequest = this.api.listRequest("pendingTest");
        if (listRequest!=null) {
            for (int i=0; i<listRequest.length; i++) {
                System.out.println(" :one: " + listRequest[i].getLabel() + "  " + listRequest[i].getTicket());
                if (ticket.length() == 0)
                    ticket = listRequest[i].getTicket();
            }
        }
        
        // Test 7: List details of one ticket of that request
        System.out.println("\nTicket Details: " + ticket + "\n");
        String requestdeleteticket = "";
        EgaTicket[] listRequestDetails = this.api.listTicketDetails(ticket);
        if (listRequestDetails!=null && listRequestDetails.length > 0) {
            System.out.println("    " + listRequestDetails[0].getTicket());
            System.out.println("    " + listRequestDetails[0].getFileName());
            System.out.println("    " + listRequestDetails[0].getFileSize());
            requestdeleteticket = listRequestDetails[0].getLabel();
        } else
            System.out.println("No Info...");

        // Test 7.0.5 Try to request a file that is pending
        // TODO
        
        // Test 7.1 Delete one ticket
        System.out.println("\nDelete one Ticket: " + requestdeleteticket + "\n");
        String[] delete_ticket = this.api.delete_ticket(requestdeleteticket, ticket);
        for (int i=0; i<delete_ticket.length; i++)
            System.out.println(delete_ticket[i]);
        
        // Test 8: Delete a Request
        System.out.println("\nDeleting One Request:\n");
        String[] delete_request = this.api.delete_request("pendingTest");
        if (delete_request!=null && delete_request.length > 0)
            System.out.println("Deleted. " + delete_request.length);
        
        // Test 8: Download a File
        //this.api.setUdt(true);
//        System.out.println("Downloading a File");
//        String[] download_netty = this.api.download_netty(t, "_ega-box-81_8622007039_R06C02_Red.idat.cip", "");
//        for (String result : download_netty)
//            System.out.println(result);
     
        
        // Done - log out again
        System.out.println("\nLogging out:\n");
        this.api.logout();
    }
    
    public static void main(String[] args) {

        testMeDB x = new testMeDB(args[0], args[1]);
        x.test();
        
    }
}
