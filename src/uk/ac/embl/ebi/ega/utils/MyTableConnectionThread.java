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

import java.io.File;
import oodforsqljet.database.SqlJetWrapper;
import oodforsqljet.database.Table;
import oodforsqljet.database.exceptions.DatabaseException;
import uk.ac.embl.ebi.ega.egadbapiwrapper.EgaDBAPIWrapper;
import uk.ac.embl.ebi.ega.sqljetdb.MyPendingRequestTable;

/**
 *
 * @author asenf
 */
public class MyTableConnectionThread implements Runnable {
    private final EgaDBAPIWrapper ref;
    private final String db_name;
    
    public MyTableConnectionThread(EgaDBAPIWrapper ref, String db_name) {
        this.ref = ref;
        this.db_name = db_name;
    }
    
    @Override
    public void run() {
        try {
            Table.associatePackageToSqlJetDatabase(MyPendingRequestTable.class.getPackage(),
                                                    new SqlJetWrapper(
                                                    new File(db_name), false));
            this.ref.setDB(true);
        } catch (DatabaseException ex) {
            System.out.println("Cache Access error: " + ex.getLocalizedMessage());
        }
        
        if (this.ref.hasDB())
            this.ref.setConnected(true);
    }
    
}
