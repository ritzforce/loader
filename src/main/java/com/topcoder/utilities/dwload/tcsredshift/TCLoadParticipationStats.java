/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.*;


public class TCLoadParticipationStats extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadParticipationStats.class);

    @Override
    public void performLoad() throws Exception {
        doLoadParticipationStats();
    }

    /**
     * <p>Loads the statistics for user participation in all types of contests: studio, software, algo and marathon.</p>
     *
     * @throws Exception if an unexpected error occurs.
     */
    public void doLoadParticipationStats() throws Exception {
        log.info("Load participation stats data");

        long start = System.currentTimeMillis();

        // Statement for selecting records from the source database
        final String SELECT
                = "SELECT rr.coder_id as user_id, rs.start_time::DATETIME YEAR TO DAY as participation_date, 1 as participation_type " +
                "FROM informixoltp:round r " +
                "INNER JOIN informixoltp:room_result rr ON  rr.round_id = r.round_id and rr.attended = 'Y' and rr.room_seed is not null " +
                "INNER JOIN informixoltp:round_segment rs ON rs.round_id = r.round_id and rs.segment_id = 1 " +
                "WHERE r.round_type_id in (1,2,10) and rs.start_time > ? " +
                "UNION " +
                "SELECT lcr.coder_id as user_id, rs.start_time::DATETIME YEAR TO DAY as participation_date, 2 as participation_type " +
                "FROM informixoltp:round r " +
                "INNER JOIN informixoltp:long_comp_result lcr ON lcr.round_id = r.round_id and lcr.attended = 'Y' " +
                "INNER JOIN informixoltp:round_segment rs ON rs.round_id = r.round_id and rs.segment_id = 1 " +
                "WHERE round_type_id in (13, 15, 19, 22, 25) and rs.start_time > ? " +
                "UNION " +
                "SELECT ri.value::int as user_id, r.create_date::DATETIME YEAR TO DAY as participation_date, " +
                "(CASE WHEN pcl.project_type_id in (1,2) THEN 3 ELSE 4 END) as participation_type " +
                "FROM resource r " +
                "INNER JOIN resource_info ri ON r.resource_id=ri.resource_id and ri.resource_info_type_id=1 " +
                "INNER JOIN project p ON r.project_id=p.project_id " +
                "INNER JOIN project_category_lu pcl ON p.project_category_id=pcl.project_category_id and pcl.project_type_id in (1,2,3) " +
                "WHERE r.resource_role_id in (1,2,4,5,6,7,8,9) and r.create_date > ? and " +
                "not exists (select 1 from contest_eligibility where r.project_id=contest_id)";

        // Statement for inserting the records to tcs_dw.participation table in target database
        final String INSERT
                = "INSERT INTO participation (user_id, participation_type, participation_date) VALUES (?,?,?)";

        PreparedStatement select = null;
        PreparedStatement insert = null;
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
            rs = select.executeQuery();

            while (rs.next()) {
                insert.clearParameters();
                insert.setLong(1, rs.getLong("user_id"));
                insert.setLong(2, rs.getLong("participation_type"));
                insert.setTimestamp(3, rs.getTimestamp("participation_date"));

                insert.executeUpdate();
                count++;
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } catch (SQLException sqle) {
            log.error("Load of Participation Stats data failed.", sqle);
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of Participation Stats data failed.\n" + sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
        }
    }

}
