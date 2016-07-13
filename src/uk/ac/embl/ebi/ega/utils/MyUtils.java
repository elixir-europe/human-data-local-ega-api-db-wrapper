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

import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 *
 * @author asenf
 */
public class MyUtils {
    
    public static String[] parseFile(String file) {
        String[] result = new String[3];
        
        StringTokenizer token = new StringTokenizer(file, "\t");
        result[0] = token.nextToken(); // name
        result[1] = token.nextToken(); // size
        result[2] = token.nextToken(); // stable_id
        
        return result;
    }
    
    public static String[][] parseFiles(String[] files) {
        String[][] result = new String[files.length][3];
        
        for (int i=0; i<files.length; i++) {
        StringTokenizer token = new StringTokenizer(files[i], "\t");
            result[i][0] = token.nextToken(); // name
            result[i][1] = token.nextToken(); // size
            result[i][2] = token.nextToken(); // stable_id
        }
        
        return result;
    }
    
    public static String[][] parseFiles(ArrayList<String> files) {
        String[][] result = new String[files.size()][3];
        
        for (int i=0; i<files.size(); i++) {
        StringTokenizer token = new StringTokenizer(files.get(i), "\t");
            result[i][0] = token.nextToken(); // name
            result[i][1] = token.nextToken(); // size
            result[i][2] = token.nextToken(); // stable_id
        }
        
        return result;
    }

    public static String[] parseTicket(String ticket) {
        String[] result = new String[3];
        
        StringTokenizer token = new StringTokenizer(ticket, "\t");
        result[0] = token.nextToken(); // Ticket
        result[1] = token.nextToken(); // Label
        result[2] = (token.hasMoreTokens())?token.nextToken():""; // file_stable_id
        
        return result;
    }
    
    public static String[][] parseTickets(String[] tickets) {
        String[][] result = new String[tickets.length][2];
        
        for (int i=0; i<tickets.length; i++) {
            StringTokenizer token = new StringTokenizer(tickets[i], "\t");
            result[i][0] = token.nextToken(); // Ticket
            result[i][1] = token.nextToken(); // Label
            result[i][2] = (token.hasMoreTokens())?token.nextToken():""; // file_stable_id
        }
        
        return result;
    }
    
    public static String[][] parseTickets(ArrayList<String> tickets) {
        String[][] result = new String[tickets.size()][2];
        
        for (int i=0; i<tickets.size(); i++) {
            StringTokenizer token = new StringTokenizer(tickets.get(i), "\t");
            result[i][0] = token.nextToken(); // Ticket
            result[i][1] = token.nextToken(); // Label
            result[i][2] = (token.hasMoreTokens())?token.nextToken():""; // file_stable_id
        }
        
        return result;
    }
}
