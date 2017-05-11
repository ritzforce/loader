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


public class TCLoadStage extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadStage.class);

    @Override
    public void performLoad() throws Exception {
        doLoadStage();
    }

    /**
     * Load the stage table
     *
     * @throws Exception
     */
    private void doLoadStage() throws Exception {
        log.info("load stage");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;

        final String SELECT =
                " select stage_id, season_id, name, start_date, end_date " +
                        " from stage " +
                        " where modify_date > ? ";


        final String UPDATE =
                "update stage set season_id=?, start_calendar_id=?, end_calendar_id=?, name=? " +
                        " where stage_id=?";

        final String INSERT =
                "insert into stage (season_id, start_calendar_id, end_calendar_id, name, stage_id) " +
                        " values (?,?,?,?,?)";


        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            int count = 0;

            select.setTimestamp(1, fLastLogTime);
            rs = select.executeQuery();

            while (rs.next()) {

                update.clearParameters();

                update.setInt(1, rs.getInt("season_id"));
                setCalendar(update, 2, rs.getTimestamp("start_date"));
                setCalendar(update, 3, rs.getTimestamp("end_date"));
                update.setString(4, rs.getString("name"));
                update.setInt(5, rs.getInt("stage_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();

                    insert.setInt(1, rs.getInt("season_id"));
                    setCalendar(insert, 2, rs.getTimestamp("start_date"));
                    setCalendar(insert, 3, rs.getTimestamp("end_date"));
                    insert.setString(4, rs.getString("name"));
                    insert.setInt(5, rs.getInt("stage_id"));

                    insert.executeUpdate();
                }
                count++;

            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'stage' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(update);
            close(select);
        }
    }

}
