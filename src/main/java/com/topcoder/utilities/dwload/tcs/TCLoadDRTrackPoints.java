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


public class TCLoadDRTrackPoints extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadDRTrackPoints.class);

    @Override
    public void performLoad() throws Exception {
        doLoadDRTrackPoints();
    }

    /**
     *
     * @throws Exception
     */
    private void doLoadDRTrackPoints() throws Exception {
        log.debug("load digital run track points");

        StringBuffer selectPointsQuery = new StringBuffer(300);
        selectPointsQuery.append(" select dp.dr_points_id, dp.track_id, dprtl.dr_points_reference_type_id, dprtl.dr_points_reference_type_desc, dpol.dr_points_operation_id, " +
                " dpol.dr_points_operation_desc, dptl.dr_points_type_id, dptl.dr_points_type_desc, dpsl.dr_points_status_id, dpsl.dr_points_status_desc, " +
                " dp.dr_points_desc, dp.user_id, dp.amount, dp.application_date, dp.award_date, dp.reference_id, dp.is_potential, " +
                " (case when dp.dr_points_reference_type_id = 2 then (select dp2.amount from dr_points dp2 where dp2.dr_points_id = dp.reference_id) else 0 end) as parent_amount  " +
                " from dr_points dp, dr_points_status_lu dpsl, dr_points_type_lu dptl, dr_points_operation_lu dpol, dr_points_reference_type_lu dprtl " +
                " where dp.dr_points_status_id = dpsl.dr_points_status_id " +
                " and dp.dr_points_type_id = dptl.dr_points_type_id " +
                " and dp.dr_points_operation_id = dpol.dr_points_operation_id " +
                " and dp.dr_points_reference_type_id = dprtl.dr_points_reference_type_id " +
                " and dp.track_id in (");

        final String INSERT =
                "insert into dr_points (dr_points_id, track_id, dr_points_reference_type_id, dr_points_reference_type_desc, dr_points_operation_id, dr_points_operation_desc, dr_points_type_id, dr_points_type_desc, dr_points_status_id, dr_points_status_desc, dr_points_desc, user_id, amount, application_date, award_date, reference_id, is_potential) " +
                        " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        final String SELECT_TRACKS =
                " select distinct track_id " +
                        " from dr_points " +
                        " where (modify_date > ? " +
                        "     OR create_date > ?) ";

        PreparedStatement selectPoints= null;
        PreparedStatement insert = prepareStatement(INSERT, TARGET_DB);
        PreparedStatement tracksSelect= prepareStatement(SELECT_TRACKS, SOURCE_DB);;
        PreparedStatement delete= null;
        ResultSet rsPoints = null;
        ResultSet tracks = null;

        int count = 0;

        try {
            long start = System.currentTimeMillis();

            StringBuffer delQuery = new StringBuffer(300);
            delQuery.append("delete from dr_points where track_id in (");

            tracksSelect.setTimestamp(1, fLastLogTime);
            tracksSelect.setTimestamp(2, fLastLogTime);

            tracks = tracksSelect.executeQuery();
            boolean tracksFound = false;
            while (tracks.next()) {
                tracksFound = true;
                delQuery.append(tracks.getLong("track_id"));
                delQuery.append(",");
                selectPointsQuery.append(tracks.getLong("track_id"));
                selectPointsQuery.append(",");
            }
            delQuery.setCharAt(delQuery.length() - 1, ')');
            selectPointsQuery.setCharAt(selectPointsQuery.length() - 1, ')');

            if (tracksFound) {
                log.debug("clean up: "+ delQuery.toString());
                delete = prepareStatement(delQuery.toString(), TARGET_DB);
                delete.executeUpdate();

                selectPoints = prepareStatement(selectPointsQuery.toString(), SOURCE_DB);

                rsPoints = selectPoints.executeQuery();
                while (rsPoints.next()) {
                    int j = 1;
                    insert.setInt(j++, rsPoints.getInt("dr_points_id"));
                    insert.setInt(j++, rsPoints.getInt("track_id"));
                    insert.setInt(j++, rsPoints.getInt("dr_points_reference_type_id"));
                    insert.setString(j++, rsPoints.getString("dr_points_reference_type_desc"));
                    insert.setInt(j++, rsPoints.getInt("dr_points_operation_id"));
                    insert.setString(j++, rsPoints.getString("dr_points_operation_desc"));
                    insert.setInt(j++, rsPoints.getInt("dr_points_type_id"));
                    insert.setString(j++, rsPoints.getString("dr_points_type_desc"));
                    insert.setInt(j++, rsPoints.getInt("dr_points_status_id"));
                    insert.setString(j++, rsPoints.getString("dr_points_status_desc"));
                    insert.setString(j++, rsPoints.getString("dr_points_desc"));
                    insert.setLong(j++, rsPoints.getLong("user_id"));
                    insert.setDouble(j++, calculatePointsAmount(rsPoints.getInt("dr_points_operation_id"),
                            rsPoints.getDouble("amount"),
                            rsPoints.getDouble("parent_amount")));
                    insert.setDate(j++, rsPoints.getDate("application_date"));
                    insert.setDate(j++, rsPoints.getDate("award_date"));
                    insert.setInt(j++, rsPoints.getInt("reference_id"));
                    insert.setBoolean(j++, rsPoints.getBoolean("is_potential"));

                    insert.executeUpdate();
                    count++;
                }
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'track points' failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rsPoints);
            close(tracks);
            close(delete);
            close(tracksSelect);
            close(insert);
            close(selectPoints);
        }

    }

    private double calculatePointsAmount(int operationType, double amount, double parentAmount) {
        if (operationType == 2) {
            return ((parentAmount * amount) / 100);
        }
        return amount;
    }

}
