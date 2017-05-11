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


public class TCLoadDirectProjectDim extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadDirectProjectDim.class);

    @Override
    public void performLoad() throws Exception {
        doLoadDirectProjectDim();
    }

    /**
     * <p>Load the direct project dimension data to DW.</p>
     *
     * @throws Exception if an unexpected error occurs.
     * @since 1.1.9
     */
    private void doLoadDirectProjectDim() throws Exception {
        log.info("Load direct project dimension data");

        long start = System.currentTimeMillis();

        // Statement for selecting the records from time_oltp table in source database
        final String SELECT
                = "SELECT project_id, name, description, project_status_id, create_date, modify_date,"  +
                "(SELECT max(dpa.billing_account_id) from corporate_oltp:direct_project_account dpa where dpa.project_id = tdp.project_id) as billing_project_id" +
                " FROM tc_direct_project tdp" +
                " WHERE tdp.modify_date > ? ";

        // Statement for updating the records in tcs_dw.client_project_dim table
        final String UPDATE = "UPDATE direct_project_dim SET name = ?, description = ?, project_status_id = ?," +
                " project_create_date = ?, project_modification_date = ?, billing_project_id = ?" +
                " WHERE direct_project_id = ?";

        // Statement for inserting the records to tcs_dw.client_project_dim table in target database
        final String INSERT
                = "INSERT INTO direct_project_dim (direct_project_id, name, description, project_status_id," +
                "                                project_create_date, project_modification_date, billing_project_id)" +
                "VALUES (?,?,?,?,?,?,?)";

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
            insert = prepareStatement(INSERT, TARGET_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            rs = select.executeQuery();

            while (rs.next()) {
                update.clearParameters();
                // name
                update.setString(1, rs.getString("name"));
                // description
                update.setString(2, rs.getString("description"));
                // project_status_id
                update.setLong(3, rs.getLong("project_status_id"));
                // project_create_date
                update.setDate(4, rs.getDate("create_date"));
                // project_modification_date
                update.setDate(5, rs.getDate("modify_date"));
                // billing_project_id
                update.setLong(6, rs.getLong("billing_project_id"));
                // direct project id
                update.setLong(7, rs.getLong("project_id"));


                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    // need to insert
                    insert.clearParameters();
                    // direct project id
                    insert.setLong(1, rs.getLong("project_id"));
                    // name
                    insert.setString(2, rs.getString("name"));
                    // description
                    insert.setString(3, rs.getString("description"));
                    // project_status_id
                    insert.setLong(4, rs.getLong("project_status_id"));
                    // project_create_date
                    insert.setDate(5, rs.getDate("create_date"));
                    // project_modification_date
                    insert.setDate(6, rs.getDate("modify_date"));
                    // billing_project_id
                    insert.setLong(7, rs.getLong("billing_project_id"));
                    insert.executeUpdate();
                }
                count++;
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } catch (SQLException sqle) {
            log.error("Load of Direct Project Dimension data failed.", sqle);
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of Direct Project Dimension data failed.\n" + sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }

}
