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

/**
 *
 * @author asenf
 */
public class MyAgentDownloadEntry {
    public String ticket;
    public String label;
    public String name;
    
    public MyAgentDownloadEntry(MyAgentDownloadEntry copy) {
        this.ticket = copy.ticket;
        this.label = copy.label;
        this.name = copy.name;
    }
    
    public MyAgentDownloadEntry(String ticket, String label, String name) {
        this.ticket = ticket;
        this.label = label;
        this.name = name;
    }
}
