/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcs;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCS;
import com.topcoder.utilities.dwload.contestresult.ContestResult;
import com.topcoder.utilities.dwload.contestresult.ProjectResult;
import com.topcoder.utilities.dwload.contestresult.TopPerformersCalculator;
import com.topcoder.utilities.dwload.contestresult.drv2.ContestResultCalculatorV2;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TCLoadDRTrackResults extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadDRTrackResults.class);

    @Override
    public void performLoad() throws Exception {
        doLoadDRTrackResults();
    }

    /**
     *
     * @throws Exception
     */
    private void doLoadDRTrackResults() throws Exception {
        log.debug("load digital run track results");

        final String SELECT_TRACKS =
                " select distinct track_id " +
                        " from dr_points " +
                        " where (modify_date > ? " +
                        "     OR create_date > ?) ";

        final String SELECT_CONTESTS =
                " select tc.track_contest_id, tctl.track_contest_type_id, tcrcl.class_name " +
                        " from track_contest tc " +
                        " ,track_contest_type_lu tctl " +
                        " ,track_contest_result_calculator_lu tcrcl " +
                        " where tc.track_contest_type_id = tctl.track_contest_type_id " +
                        " and tc.track_contest_result_calculator_id = tcrcl.track_contest_result_calculator_id " +
                        " and tc.track_id = ? ";


        PreparedStatement selectTracks = null;
        PreparedStatement selectContests = null;
        ResultSet rsTracks = null;
        ResultSet rsContests = null;

        try {
            selectTracks = prepareStatement(SELECT_TRACKS, SOURCE_DB);
            selectContests = prepareStatement(SELECT_CONTESTS, SOURCE_DB);

            selectTracks.setTimestamp(1, fLastLogTime);
            selectTracks.setTimestamp(2, fLastLogTime);

            rsTracks = selectTracks.executeQuery();
            int trackId = 0;
            while (rsTracks.next()) {
                trackId = rsTracks.getInt("track_id");
                selectContests.clearParameters();
                selectContests.setInt(1, trackId);
                rsContests = selectContests.executeQuery();

                while (rsContests.next()) {
                    loadTrackContestResults(trackId,
                            rsContests.getInt("track_contest_id"),
                            rsContests.getInt("track_contest_type_id"),
                            rsContests.getString("class_name"));
                }

            }

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'track results' failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rsContests);
            close(rsTracks);
            close(selectContests);
            close(selectTracks);
        }

    }

    private void loadTrackContestResults(int trackId, int trackContestId, int trackContestTypeId,
                                         String calculatorClassName) throws Exception {
        log.debug("loading track results for track =" + trackId + ", contest=" + trackContestId + ", trackContestTypeId=" + trackContestTypeId);

        final String SELECT_POINTS =
                " select dp.user_id, dp.amount, dp.is_potential, " +
                        " (case when dp.dr_points_reference_type_id = 1 then (select pr.placed from project_result pr where pr.user_id = dp.user_id and pr.project_id = dp.reference_id) else 0 end) as placed, " +
                        " (case when dp.dr_points_reference_type_id = 1 then (select pr.final_score from project_result pr where pr.user_id = dp.user_id and pr.project_id = dp.reference_id) else 0 end) as final_score, " +
                        " (case when dp.dr_points_reference_type_id = 1 then (select (case when pr.payment is null or pr.payment=0 then 't' else 'f' end) from project_result pr where " +
                        " pr.user_id = dp.user_id and pr.project_id = dp.reference_id) else 'f' end) as taxable " +
                        " from dr_points dp where dp.track_id = ? ";

        final String INSERT =
                "insert into track_contest_results (track_contest_id, user_id, track_contest_placement, track_contest_prize, taxable_track_contest_prize) " +
                        " values (?,?,?,?,?)";

        PreparedStatement insert = prepareStatement(INSERT, TARGET_DB);

        ResultSet rs = null;
        PreparedStatement selectPoints = null;

        long start = System.currentTimeMillis();

        try {
            selectPoints = prepareStatement(SELECT_POINTS, TARGET_DB);

            ContestResultCalculatorV2 calc = (ContestResultCalculatorV2) Class.forName(calculatorClassName).newInstance();
            if (calc instanceof TopPerformersCalculator) {
                ((TopPerformersCalculator) calc).setFactor(2);
            }

            selectPoints.setInt(1, trackId);
            rs = selectPoints.executeQuery();

            Map<Long, ContestResult> results = new HashMap<Long, ContestResult>();
            while (rs.next()) {
                ContestResult cr = results.get(rs.getLong("user_id"));

                if (cr == null)  {
                    cr = new ContestResult(rs.getLong("user_id"));
                    results.put(rs.getLong("user_id"), cr);
                }

                if (rs.getBoolean("is_potential")) {
                    cr.addPotentialPoints(rs.getDouble("amount"));
                } else {
                    cr.addPoints(rs.getDouble("amount"), rs.getBoolean("taxable"));
                }

                if (rs.getInt("placed") > 0) {
                    cr.addResult(new ProjectResult(rs.getDouble("final_score"), rs.getInt("placed")));
                }
            }
            close(rs);

            List<ContestResult> finalResults = calc.calculateResults(new ArrayList<ContestResult>(results.values()));

            simpleDelete("track_contest_results", "track_contest_id", trackContestId);
            int count = 0;
            log.debug("Results...");
            log.debug("==========");
            for (ContestResult result : finalResults) {
                log.debug("" + result.getPlace() + ") " + result.getCoderId() + " | " + result.getFinalPoints() + "|" + result.getPotentialPoints() + "|" + (result.getPrize() != null ? result.getPrize() : ""));
                insert.setInt(1, trackContestId);
                insert.setLong(2, result.getCoderId());
                insert.setInt(3, result.getPlace());
                if (result.getPrize() == null) {
                    insert.setNull(4, Types.DOUBLE);
                    insert.setNull(5, Types.DOUBLE);
                } else {
                    insert.setDouble(4, result.getPrize());
                    insert.setDouble(5, result.getPrize() * (result.getTaxableFinalPoints() / result.getFinalPoints()));
                }
                insert.executeUpdate();
                count++;
            }
            log.debug("==========");
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } finally {
            close(rs);
            close(selectPoints);
            close(insert);
        }
    }

}
