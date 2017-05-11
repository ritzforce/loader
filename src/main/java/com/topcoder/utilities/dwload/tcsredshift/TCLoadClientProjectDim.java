/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.*;


public class TCLoadClientProjectDim extends TCLoadTCSRedshift {

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
    public void doLoadClientProjectDim() throws Exception {
        log.info("Load client project dimension data");

        long start = System.currentTimeMillis();

        // Statement for selecting the records from time_oltp table in source database
        final String SELECT
                = "SELECT a.client_id, a.name as client_name, a.creation_date as client_create_date, a.modification_date as client_modification_date, " +
                " b.project_id as billing_project_id, b.name as project_name, b.creation_date as project_create_date, b.modification_date as project_modification_date, " +
                " b.po_box_number as billing_account_code, a.cmc_account_id, a.customer_number, b.active as billing_account_status, " +
                " b.start_date as billing_account_start_date, b.end_date as billing_account_end_date " +
                " FROM time_oltp:client a, time_oltp:project b, time_oltp:client_project c" +
                " WHERE c.client_id = a.client_id AND c.project_id = b.project_id" +
                "  AND (a.modification_date > ? OR b.modification_date > ? OR c.modification_date > ?)";

        // Statement for updating the records in tcs_dw.client_project_dim table
        final String UPDATE = "UPDATE client_project_dim SET client_name = ?, client_create_date = ?, client_modification_date = ?, " +
                "project_name = ?, project_create_date = ?, project_modification_date = ?, billing_account_code = ? , client_id = ?, cmc_account_id = ?, customer_number = ?, " +
                "billing_account_status = ?, billing_account_start_date = ?, billing_account_end_date = ? " +
                "WHERE billing_project_id = ?";

        // Statement for inserting the records to tcs_dw.client_project_dim table in target database
        final String INSERT
                = "INSERT INTO client_project_dim (client_id, client_name, client_create_date, client_modification_date," +
                "                                billing_project_id, project_name, project_create_date, project_modification_date, billing_account_code, client_project_id, cmc_account_id, customer_number, " +
                "                                billing_account_status, billing_account_start_date, billing_account_end_date)" +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
                update.setTimestamp(2, rs.getTimestamp("client_create_date"));
                // client modification date
                update.setTimestamp(3, rs.getTimestamp("client_modification_date"));
                // project name
                update.setString(4, rs.getString("project_name"));
                // project creation date
                update.setTimestamp(5, rs.getTimestamp("project_create_date"));
                // project modification date
                update.setTimestamp(6, rs.getTimestamp("project_modification_date"));
                // billing account code
                update.setString(7, rs.getString("billing_account_code"));
                // client id
                update.setLong(8, rs.getLong("client_id"));
                // cmc account id
                update.setString(9, rs.getString("cmc_account_id"));
                // customer number
                update.setString(10, rs.getString("customer_number"));

                update.setString(11, rs.getString("billing_account_status"));

                update.setTimestamp(12, rs.getTimestamp("billing_account_start_date"));

                update.setTimestamp(13, rs.getTimestamp("billing_account_end_date"));

                // billing project id
                update.setLong(14, rs.getLong("billing_project_id"));


                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    // need to insert
                    insert.clearParameters();
                    // client id
                    insert.setLong(1, rs.getLong("client_id"));
                    // client name
                    insert.setString(2, rs.getString("client_name"));
                    // client creation date
                    insert.setTimestamp(3, rs.getTimestamp("client_create_date"));
                    // client modification date
                    insert.setTimestamp(4, rs.getTimestamp("client_modification_date"));
                    // billing project id
                    insert.setLong(5, rs.getLong("billing_project_id"));
                    // project name
                    insert.setString(6, rs.getString("project_name"));
                    // project creation date
                    insert.setTimestamp(7, rs.getTimestamp("project_create_date"));
                    // project modification date
                    insert.setTimestamp(8, rs.getTimestamp("project_modification_date"));
                    // billing account code
                    insert.setString(9, rs.getString("billing_account_code"));
                    // billing project id as client project id
                    insert.setLong(10, rs.getLong("billing_project_id"));
                    // billing account code
                    insert.setString(11, rs.getString("cmc_account_id"));
                    // customer number
                    insert.setString(12, rs.getString("customer_number"));     System.out.println("------billing_project_id--------"+rs.getLong("billing_project_id"));

                    insert.setString(13, rs.getString("billing_account_status"));

                    insert.setTimestamp(14, rs.getTimestamp("billing_account_start_date"));

                    insert.setTimestamp(15, rs.getTimestamp("billing_account_end_date"));

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
