/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class TCLoadSeason extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadSeason.class);

    @Override
    public void performLoad() throws Exception {
        doLoadSeason();
    }

    /**
     * Load the season table
     *
     * @throws Exception
     */
    private void doLoadSeason() throws Exception {
        log.info("load season");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;

        final String SELECT =
                " select season_id, name, rookie_competition_ind, next_rookie_season_id " +
                        "       , (select min(start_date) from stage st where st.season_id = s.season_id) as start_date " +
                        "       , (select max(end_date) from stage st where st.season_id = s.season_id) as end_date " +
                        " from season s " +
                        " where modify_date > ? ";


        final String UPDATE =
                "update season set start_calendar_id=?, end_calendar_id=?, name=?, rookie_competition_ind=?, next_rookie_season_id=? " +
                        " where season_id=?";

        final String INSERT =
                "insert into season (start_calendar_id, end_calendar_id, name, rookie_competition_ind, next_rookie_season_id, season_id) " +
                        " values (?,?,?,?,?,?)";


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

                setCalendar(update, 1, rs.getTimestamp("start_date"));
                setCalendar(update, 2, rs.getTimestamp("end_date"));
                update.setString(3, rs.getString("name"));
                update.setInt(4, rs.getInt("rookie_competition_ind"));
                update.setInt(5, rs.getInt("next_rookie_season_id"));
                update.setInt(6, rs.getInt("season_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();

                    setCalendar(insert, 1, rs.getTimestamp("start_date"));
                    setCalendar(insert, 2, rs.getTimestamp("end_date"));
                    insert.setString(3, rs.getString("name"));
                    insert.setInt(4, rs.getInt("rookie_competition_ind"));
                    insert.setInt(5, rs.getInt("next_rookie_season_id"));
                    insert.setInt(6, rs.getInt("season_id"));

                    insert.executeUpdate();
                }
                count++;

            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'season' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(update);
            close(select);
        }
    }

}
