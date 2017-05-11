/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcs;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


public class TCLoadClientProjectDim extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadClientProjectDim.class);

    @Override
    public void performLoad() throws Exception {
        doLoadClientProjectDim();
    }

    /**
     * <p>Load the client project dimension data to DW.</p>
     *
     * @throws Exception if an unexpected error occurs.
     * @since 1.1.9
     */
    private void doLoadClientProjectDim() throws Exception {
        log.info("Load client project dimension data");

        long start = System.currentTimeMillis();

        // Statement for selecting the records from time_oltp table in source database
        final String SELECT
                = "SELECT a.client_id, a.name as client_name, a.creation_date as client_create_date, a.modification_date as client_modification_date, " +
                " b.project_id as billing_project_id, b.name as project_name, b.creation_date as project_create_date, b.modification_date as project_modification_date, " +
                " b.po_box_number as billing_account_code, a.cmc_account_id, a.customer_number " +
                " FROM time_oltp:client a, time_oltp:project b, time_oltp:client_project c" +
                " WHERE c.client_id = a.client_id AND c.project_id = b.project_id" +
                "  AND (a.modification_date > ? OR b.modification_date > ? OR c.modification_date > ?)";

        // Statement for updating the records in tcs_dw.client_project_dim table
        final String UPDATE = "UPDATE client_project_dim SET client_name = ?, client_create_date = ?, client_modification_date = ?, " +
                "project_name = ?, project_create_date = ?, project_modification_date = ?, billing_account_code = ? , client_id = ?, cmc_account_id = ?, customer_number = ? " +
                "WHERE billing_project_id = ?";

        // Statement for inserting the records to tcs_dw.client_project_dim table in target database
        final String INSERT
                = "INSERT INTO client_project_dim (client_id, client_name, client_create_date, client_modification_date," +
                "                                billing_project_id, project_name, project_create_date, project_modification_date, billing_account_code, client_project_id, cmc_account_id, customer_number)" +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;
        ResultSet rs = null;
        int count = 0;

        try {
            select = prepareStatement(SELECT, SOURCE_DB);
            if (fLastLogTime == null) {
                fLastLogTime = new Timestamp(0);
            }
            select.setTimestamp(1, fLastLogTime);
            select.setTimestamp(2, fLastLogTime);
            select.setTimestamp(3, fLastLogTime);
            insert = prepareStatement(INSERT, TARGET_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            rs = select.executeQuery();


            while (rs.next()) {
                update.clearParameters();
                // client_name
                update.setString(1, rs.getString("client_name"));
                // client creation date
                update.setDate(2, rs.getDate("client_create_date"));
                // client modification date
                update.setDate(3, rs.getDate("client_modification_date"));
                // project name
                update.setString(4, rs.getString("project_name"));
                // project creation date
                update.setDate(5, rs.getDate("project_create_date"));
                // project modification date
                update.setDate(6, rs.getDate("project_modification_date"));
                // billing account code
                update.setString(7, rs.getString("billing_account_code"));
                // client id
                update.setLong(8, rs.getLong("client_id"));
                // cmc account id
                update.setString(9, rs.getString("cmc_account_id"));
                // customer number
                update.setString(10, rs.getString("customer_number"));

                // billing project id
                update.setLong(11, rs.getLong("billing_project_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    // need to insert
                    insert.clearParameters();
                    // client id
                    insert.setLong(1, rs.getLong("client_id"));
                    // client name
                    insert.setString(2, rs.getString("client_name"));
                    // client creation date
                    insert.setDate(3, rs.getDate("client_create_date"));
                    // client modification date
                    insert.setDate(4, rs.getDate("client_modification_date"));
                    // billing project id
                    insert.setLong(5, rs.getLong("billing_project_id"));
                    // project name
                    insert.setString(6, rs.getString("project_name"));
                    // project creation date
                    insert.setDate(7, rs.getDate("project_create_date"));
                    // project modification date
                    insert.setDate(8, rs.getDate("project_modification_date"));
                    // billing account code
                    insert.setString(9, rs.getString("billing_account_code"));
                    // billing project id as client project id
                    insert.setLong(10, rs.getLong("billing_project_id"));
                    // billing account code
                    insert.setString(11, rs.getString("cmc_account_id"));
                    // customer number
                    insert.setString(12, rs.getString("customer_number"));     System.out.println("------billing_project_id--------"+rs.getLong("billing_project_id"));
                    insert.executeUpdate();
                }
                count++;
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } catch (SQLException sqle) {
            log.error("Load of Client Project Dimension data failed.", sqle);
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of Client Project Dimension data failed.\n" + sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }

}
