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


public class TCLoadDRTrackContests extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadDRTrackContests.class);

    @Override
    public void performLoad() throws Exception {
        doLoadDRTrackContests();
    }

    /**
     *
     * @throws Exception
     */
    public void doLoadDRTrackContests() throws Exception {
        log.debug("load digital run track contests");

        final String SELECT_CONTESTS =
                " select tc.track_contest_id, tc.track_id, tc.track_contest_desc, tctl.track_contest_type_id, tctl.track_contest_type_desc " +
                        " from track_contest tc, track_contest_type_lu tctl " +
                        " where tc.track_contest_type_id = tctl.track_contest_type_id " +
                        " and tc.create_date > ?";

        final String UPDATE =
                "update track_contest set track_id = ?, track_contest_desc = ?, track_contest_type_id = ?, track_contest_type_desc = ? " +
                        " where track_contest_id = ?";

        final String INSERT =
                "insert into track_contest (track_contest_id, track_id, track_contest_desc, track_contest_type_id, track_contest_type_desc) " +
                        " values (?,?,?,?,?)";

        PreparedStatement selectContests = prepareStatement(SELECT_CONTESTS, SOURCE_DB);
        PreparedStatement update = prepareStatement(UPDATE, TARGET_DB);
        PreparedStatement insert = prepareStatement(INSERT, TARGET_DB);
        ResultSet rsContests = null;

        int count = 0;

        try {
            long start = System.currentTimeMillis();

            selectContests.setTimestamp(1, fLastLogTime);

            rsContests = selectContests.executeQuery();
            while (rsContests.next()) {
                update.clearParameters();
                update.setInt(1, rsContests.getInt("track_id"));
                update.setString(2, rsContests.getString("track_contest_desc"));
                update.setInt(3, rsContests.getInt("track_contest_type_id"));
                update.setString(4, rsContests.getString("track_contest_type_desc"));
                update.setInt(5, rsContests.getInt("track_contest_id"));

                int retVal = update.executeUpdate();
                if (retVal == 0) {
                    insert.clearParameters();
                    insert.setInt(1, rsContests.getInt("track_contest_id"));
                    insert.setInt(2, rsContests.getInt("track_id"));
                    insert.setString(3, rsContests.getString("track_contest_desc"));
                    insert.setInt(4, rsContests.getInt("track_contest_type_id"));
                    insert.setString(5, rsContests.getString("track_contest_type_desc"));

                    insert.executeUpdate();
                }

                count++;
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'track contests' failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rsContests);
            close(update);
            close(insert);
            close(selectContests);
        }

    }

}
