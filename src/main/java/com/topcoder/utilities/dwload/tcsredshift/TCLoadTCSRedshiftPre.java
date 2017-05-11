/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCS;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TCLoadTCSRedshiftPre extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadTCSRedshiftPre.class);

    @Override
    public void performLoad() throws Exception {
        log.info("[TCS -> REDSHIFT LOAD] TCS to Redshift load running");

        PreparedStatement ps = null;
        try {
            ps = prepareStatement("set lock mode to wait 5", SOURCE_DB);
            ps.execute();
        } finally {
            close(ps);
        }

        getLastUpdateTime();
    }

    private void getLastUpdateTime() throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        StringBuffer query;

        query = new StringBuffer(100);
        query.append("select log_timestamp from update_log where log_id = ");
        query.append("(select max(log_id) from update_log where log_type_id = " + TCS_LOG_TYPE + ")");

        try {
            stmt = createStatement(TARGET_DB);
            rs = stmt.executeQuery(query.toString());
            if (rs.next()) {
                fLastLogTime = rs.getTimestamp(1);
                log.info("Date is " + fLastLogTime.toString());
            } else {
                // A little misleading here as we really didn't hit a SQL
                // exception but all we are doing outside this method is
                // catching and setting the reason for failure to be the
                // message of the exception.
                throw new SQLException("Last log time not found in " +
                        "update_log table.");
            }
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Failed to retrieve last log time.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
    }

}
