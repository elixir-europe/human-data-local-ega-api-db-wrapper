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
import oodforsqljet.database.exceptions.DatabaseException;

/**
 *
 * @author asenf
 */
public final class MyPendingRequestTable extends Table<MyPendingRequestTable.Record> {
    
    protected MyPendingRequestTable() throws DatabaseException {
        super();
    }

    public MyPendingRequestTable.Record addRecord(String label, long date, String file_stable_id, String file_name, String dataset_stable_id) throws DatabaseException {
        HashMap<String, Object> fields=new HashMap<>();
        fields.put("label",label);
        fields.put("date",date);
        fields.put("file_stable_id",file_stable_id);
        fields.put("file_name",file_name);
        fields.put("dataset_stable_id",dataset_stable_id);
        ArrayList<MyPendingRequestTable.Record> records = getRecords(label, file_stable_id);
        if (records==null || records.isEmpty())
            return super.addRecord(fields);
        else
            return null;
    }

    public ArrayList<MyPendingRequestTable.Record> getRecords(String label, String file_stable_id) {
        try {
            HashMap<String, Object> fields=new HashMap<>();
            fields.put("label",label);
            fields.put("file_stable_id",file_stable_id);
            return super.getRecordsWithAllFields(fields);
        } catch (DatabaseException ex) {
            System.err.println(ex.toString());
            return null;
        }
    }

    public ArrayList<String> getPendingFiles(String label) throws DatabaseException {
        ArrayList<String> files = new ArrayList<>();

        ArrayList<MyPendingRequestTable.Record> records = MyPendingRequestTable.this.getRecords();
        for (MyPendingRequestTable.Record r : records) {
            if (r.label.equalsIgnoreCase(label))
              files.add(r.file_stable_id);
        }
        
        return files;
    }
    
    public ArrayList<MyPendingRequestTable.Record> getPendingFileRecords(String label) throws DatabaseException {
        ArrayList<MyPendingRequestTable.Record> files = new ArrayList<>();

        ArrayList<MyPendingRequestTable.Record> records = MyPendingRequestTable.this.getRecords();
        for (MyPendingRequestTable.Record r : records) {
            if (r.label.equalsIgnoreCase(label))
              files.add(r);
        }
        
        return files;
    }

    public ArrayList<String> getPendingLabels() throws DatabaseException {
        ArrayList<String> files = new ArrayList<>();

        ArrayList<MyPendingRequestTable.Record> records = MyPendingRequestTable.this.getRecords();
        for (MyPendingRequestTable.Record r : records) {
            if (!files.contains(r.label))
                files.add(r.label);
        } 
        
        return files;
    }

    public ArrayList<String> getPendingDatasets() throws DatabaseException {
        ArrayList<String> datasets = new ArrayList<>();

        ArrayList<MyPendingRequestTable.Record> records = MyPendingRequestTable.this.getRecords();
        for (MyPendingRequestTable.Record r : records) {
            if (!datasets.contains(r.dataset_stable_id))
                datasets.add(r.dataset_stable_id);
        } 
        
        return datasets;
    }
    
    public long removeRecord(Long id_request) throws DatabaseException {
        HashMap<String, Object> fields=new HashMap<>();
        fields.put("id_request",id_request);
        return super.removeRecordsWithOneOfFields(fields);
    }

    public long removeRequest(String label) throws DatabaseException {
        HashMap<String, Object> fields=new HashMap<>();
        fields.put("label",label);
        return super.removeRecordsWithOneOfFields(fields);
    }
    
    public static class Record extends DatabaseRecord {
        protected Record() {
            super();
        }
  
        public @AutoPrimaryKey long id_request;
        public @Field @NotNull String label;
        public @Field @NotNull long date;
        public @Field @NotNull String file_stable_id;
        public @Field @NotNull String file_name;
        public @Field @NotNull String dataset_stable_id;
    }    
}
