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
package uk.ac.embl.ebi.ega.sqljetdb;

import java.util.ArrayList;
import java.util.HashMap;
import oodforsqljet.database.DatabaseRecord;
import oodforsqljet.database.Table;
import oodforsqljet.database.annotations.AutoPrimaryKey;
import oodforsqljet.database.annotations.Field;
import oodforsqljet.database.annotations.NotNull;
import oodforsqljet.database.annotations.PrimaryKey;
import oodforsqljet.database.exceptions.DatabaseException;

/**
 *
 * @author asenf
 */
public final class MyRequestTable extends Table<MyRequestTable.Record> {
    
    protected MyRequestTable() throws DatabaseException {
        super();
    }

    public MyRequestTable.Record addRecord(String label, long date, String ticket, String file_stable_id, String file_name, String path, String ip) throws DatabaseException {
        HashMap<String, Object> fields=new HashMap<>();
        fields.put("label",label);
        fields.put("date",date);
        fields.put("ticket",ticket);
        fields.put("file_stable_id",file_stable_id);
        fields.put("file_name",file_name);
        fields.put("path",path);
        fields.put("ip", ip);
        ArrayList<MyRequestTable.Record> records = getRecords(ticket);
        if (records==null || records.isEmpty())
            return super.addRecord(fields);
        else
            return null;
    }
    
    public ArrayList<MyRequestTable.Record> getRecords(String ticket) {
        try {
            HashMap<String, Object> fields=new HashMap<>();
            fields.put("ticket",ticket);
            return super.getRecordsWithAllFields(fields);
        } catch (DatabaseException ex) {
            return null;
        }
    }

    public ArrayList<MyRequestTable.Record> getRecordsByLabel(String label, String ip) {
        try {
            HashMap<String, Object> fields=new HashMap<>();
            fields.put("label",label);
            fields.put("ip", ip);
            return super.getRecordsWithAllFields(fields);
        } catch (DatabaseException ex) {
            return null;
        }
    }

    public ArrayList<MyRequestTable.Record> getRecordsByIp(String ip) {
        try {
            HashMap<String, Object> fields=new HashMap<>();
            fields.put("ip", ip);
            return super.getRecordsWithOneOfFields(fields);
        } catch (DatabaseException ex) {
            return null;
        }
    }

    public String getTicketFile(String ticket) throws DatabaseException {
        ArrayList<MyRequestTable.Record> records = MyRequestTable.this.getRecords();
        for (MyRequestTable.Record r : records) {
            if (r.ticket.equalsIgnoreCase(ticket)) {
                return r.file_stable_id;
            }
        }
        
        return "";
    }
    
    public long removeRecord(String ticket) throws DatabaseException {
        HashMap<String, Object> fields=new HashMap<>();
        fields.put("ticket",ticket);
        return super.removeRecordsWithOneOfFields(fields);
    }
    
    public static class Record extends DatabaseRecord {
        protected Record() {
            super();
        }
  
        public @AutoPrimaryKey              long id_ticket;
        public @Field @NotNull              String label;
        public @Field @NotNull              long date;
        public @Field @NotNull @PrimaryKey  String ticket;
        public @Field @NotNull              String file_stable_id;
        public @Field @NotNull              String file_name;
        public @Field @NotNull              String path;
        public @Field @NotNull              String ip;
    }    
}
