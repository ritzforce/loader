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


public class TCLoadDRTracks extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadDRTracks.class);

    @Override
    public void performLoad() throws Exception {
        doLoadDRTracks();
    }

    /**
     *
     * @throws Exception
     */
    private void doLoadDRTracks() throws Exception {
        log.debug("load digital run tracks");

        final String SELECT_TRACKS =
                " select t.track_id, ttl.track_type_id, ttl.track_type_desc, tsl.track_status_id, tsl.track_status_desc, " +
                        " t.track_desc, t.track_start_date, t.track_end_date " +
                        " from track t, track_status_lu tsl, track_type_lu ttl " +
                        " where t.track_status_id = tsl.track_status_id " +
                        " and t.track_type_id = ttl.track_type_id " +
                        " and t.create_date > ?";

        final String UPDATE =
                "update track set track_type_id = ?, track_type_desc = ?, track_status_id = ?, track_status_desc = ?, track_desc = ?, track_start_date = ?, track_end_date = ? " +
                        " where track_id = ?";

        final String INSERT =
                "insert into track (track_id, track_type_id, track_type_desc, track_status_id, track_status_desc, track_desc, track_start_date, track_end_date) " +
                        " values (?,?,?,?,?,?,?,?)";

        PreparedStatement selectTracks = prepareStatement(SELECT_TRACKS, SOURCE_DB);
        PreparedStatement update = prepareStatement(UPDATE, TARGET_DB);
        PreparedStatement insert = prepareStatement(INSERT, TARGET_DB);
        ResultSet rsTracks = null;

        int count = 0;

        try {
            long start = System.currentTimeMillis();

            selectTracks.setTimestamp(1, fLastLogTime);

            rsTracks = selectTracks.executeQuery();
            while (rsTracks.next()) {

                update.clearParameters();
                update.setInt(1, rsTracks.getInt("track_type_id"));
                update.setString(2, rsTracks.getString("track_type_desc"));
                update.setInt(3, rsTracks.getInt("track_status_id"));
                update.setString(4, rsTracks.getString("track_status_desc"));
                update.setString(5, rsTracks.getString("track_desc"));
                update.setDate(6, rsTracks.getDate("track_start_date"));
                update.setDate(7, rsTracks.getDate("track_end_date"));
                update.setInt(8, rsTracks.getInt("track_id"));

                int retVal = update.executeUpdate();
                if (retVal == 0) {
                    insert.clearParameters();
                    insert.setInt(1, rsTracks.getInt("track_id"));
                    insert.setInt(2, rsTracks.getInt("track_type_id"));
                    insert.setString(3, rsTracks.getString("track_type_desc"));
                    insert.setInt(4, rsTracks.getInt("track_status_id"));
                    insert.setString(5, rsTracks.getString("track_status_desc"));
                    insert.setString(6, rsTracks.getString("track_desc"));
                    insert.setDate(7, rsTracks.getDate("track_start_date"));
                    insert.setDate(8, rsTracks.getDate("track_end_date"));

                    insert.executeUpdate();
                }

                count++;
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'tracks' failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rsTracks);
            close(insert);
            close(update);
            close(selectTracks);
        }

    }

}
