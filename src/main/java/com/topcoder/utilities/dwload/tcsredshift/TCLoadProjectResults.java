/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;
import com.topcoder.utilities.dwload.contestresult.ContestResultCalculator;
import com.topcoder.utilities.dwload.contestresult.ProjectResult;
import com.topcoder.utilities.dwload.contestresult.drv2.ContestResultCalculatorV2;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TCLoadProjectResults extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadProjectResults.class);

    @Override
    public void performLoad() throws Exception {
        doLoadProjectResults();
    }

    /**
     * <p/>
     * Load projects results to the DW.
     * </p>
     *
     * @throws Exception if any error occurs
     */
    public void doLoadProjectResults() throws Exception {
        log.info("load project results");
        ResultSet projectResults = null;
        PreparedStatement projectSelect = null;
        PreparedStatement resultInsert = null;
        PreparedStatement drInsert = null;
        PreparedStatement resultSelect = null;
        PreparedStatement delete = null;
        PreparedStatement deleteDrPoints = null;
        PreparedStatement dwDataSelect = null;
        PreparedStatement dwDataUpdate = null;
        PreparedStatement psNumRatings = null;
        ResultSet numRatings = null;
        ResultSet projects = null;
        ResultSet dwData = null;

        final String PROJECTS_SELECT =
                "select distinct pr.project_id " +
                        "from project_result pr, " +
                        "project p, " +
                        "project_info pi, " +
                        "comp_versions cv, " +
                        "comp_catalog cc " +
                        "where p.project_id = pr.project_id " +
                        "and p.project_id = pi.project_id " +
                        "and p.project_status_id <> 3 " +
                        "and p.project_category_id in " + LOAD_CATEGORIES +
                        "and pi.project_info_type_id = 1 " +
                        "and cv.comp_vers_id= pi.value " +
                        "and cc.component_id = cv.component_id " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        "and (p.modify_date > ? " +
                        "   OR cv.modify_date > ? " +
                        "   OR pi.modify_date > ? " +
                        "   OR cc.modify_date > ? " +
                        "   OR pr.modify_date > ?" +
                        (needLoadMovedProject() ? " OR p.modify_user <> 'Converter' " +
                                " OR pi.modify_user <> 'Converter' " +
                                ")"
                                : ")");

        final String RESULT_SELECT = "SELECT DISTINCT pr.project_id,  " +
                " pr.user_id,  " +
                " CASE WHEN EXISTS (  " +
                "    SELECT '1'  " +
                "    FROM submission s,  " +
                "     upload u  " +
                "    WHERE s.submission_type_id = 1 AND u.resource_id = r.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id AND s  " +
                "     .submission_status_id IN (1, 2, 3, 4)  " +
                "    ) THEN 1 ELSE 0 END AS submit_ind,  " +
                " CASE WHEN p.project_category_id = 38 THEN (  " +
                "    CASE WHEN EXISTS (  " +
                "       SELECT '1'  " +
                "       FROM submission s,  " +
                "        upload u  " +
                "       WHERE s.submission_type_id = 1 AND u.resource_id = r.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id and s.placement = 1 and s.final_score = 100  " +
                "        AND submission_status_id IN (1, 2, 3, 4)  " +
                "       ) THEN 1 ELSE 0 END    " +
                "    " +
                " ) ELSE (  " +
                "    CASE WHEN EXISTS (  " +
                "       SELECT '1'  " +
                "       FROM submission s,  " +
                "        upload u  " +
                "       WHERE s.submission_type_id = 1 AND u.resource_id = r.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id   " +
                "        AND submission_status_id IN (1, 2, 3, 4)  " +
                "       ) THEN pr.valid_submission_ind ELSE 0 END  " +
                "    ) END AS valid_submission_ind,  " +
                "      " +
                "CASE WHEN p.project_category_id = 38 THEN (  " +
                "       SELECT MAX(s.initial_score)  " +
                "       FROM submission s,  " +
                "        upload u  " +
                "       WHERE s.submission_type_id = 1 AND u.resource_id = r.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id and s.placement = 1  " +
                "       AND submission_status_id IN (1, 2, 3, 4)  " +
                "    " +
                " ) ELSE (  " +
                "   " +
                "    pr.raw_score  " +
                "     " +
                ") END AS raw_score,      " +
                "  " +
                "CASE WHEN p.project_category_id = 38 THEN (  " +
                "     SELECT MAX(s.final_score)  " +
                "       FROM submission s,  " +
                "        upload u  " +
                "       WHERE s.submission_type_id = 1 AND u.resource_id = r.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id and s.placement = 1  " +
                "       AND submission_status_id IN (1, 2, 3, 4)  " +
                "    " +
                " ) ELSE (  " +
                "   " +
                "    pr.final_score  " +
                "     " +
                ") END AS final_score,       " +
                "  " +
                " (  " +
                "  SELECT max(create_time)  " +
                "  FROM component_inquiry  " +
                "  WHERE project_id = p.project_id AND user_id = pr.user_id  " +
                "  ) AS inquire_timestamp,  " +
                " r2.value registrationd_date,  " +
                " (  " +
                "  SELECT max(u.create_date)  " +
                "  FROM submission s,  " +
                "   upload u  " +
                "  WHERE s.submission_type_id = 1 AND r.resource_id = u.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id AND   " +
                "   submission_status_id <> 5  " +
                "  ) AS submit_timestamp,  " +
                " (  " +
                "  SELECT max(rev.modify_date)  " +
                "  FROM review rev,  " +
                "   scorecard s,  " +
                "   submission sub,  " +
                "   upload u  " +
                "  WHERE sub.submission_type_id = 1 AND r.resource_id = u.resource_id AND u.upload_id = sub.upload_id AND sub.submission_id = rev.  " +
                "   submission_id AND rev.scorecard_id = s.scorecard_id AND s.scorecard_type_id IN (2,8) AND rev.COMMITTED = 1 AND u.project_id = pr.  " +
                "   project_id AND sub.submission_status_id <> 5  " +
                "  ) AS review_completed_timestamp,  " +
                " (  " +
                "  SELECT count(*)  " +
                "  FROM submission s,  " +
                "   upload u  " +
                "  WHERE s.submission_type_id = 1 AND u.upload_id = s.upload_id AND project_id = p.project_id AND submission_status_id IN (1, 4)  " +
                "  ) AS num_submissions_passed_review,  " +
                "   " +
                " CASE WHEN p.project_category_id = 38 THEN (  " +
                "    " +
                "       SELECT prz.prize_amount  " +
                "       FROM submission s,  " +
                "        upload u, prize prz  " +
                "       WHERE s.submission_type_id = 1 AND u.resource_id = r.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id and s.placement = 1 and s.final_score = 100  " +
                "        AND submission_status_id IN (1, 2, 3, 4) and s.prize_id = prz.prize_id  " +
                "      " +
                "    " +
                " ) ELSE (  " +
                "     pr.payment  " +
                "    ) END AS payment,  " +
                "   " +
                " pr.old_rating,  " +
                " pr.new_rating,  " +
                " pre.reliability_before_resolution,  " +
                " pre.reliability_after_resolution,  " +
                " pr.placed,  " +
                " pr.rating_ind,  " +
                "  " +
                "   " +
                " CASE WHEN p.project_category_id = 38 THEN (  " +
                "    CASE WHEN EXISTS (  " +
                "       SELECT '1'  " +
                "       FROM submission s,  " +
                "        upload u  " +
                "       WHERE s.submission_type_id = 1 AND u.resource_id = r.resource_id AND u.upload_id = s.upload_id AND u.project_id = pr.project_id and s.placement = 1 and s.final_score = 100  " +
                "        AND submission_status_id IN (1, 2, 3, 4)  " +
                "       ) THEN 1 ELSE 0 END    " +
                "    " +
                " ) ELSE (  " +
                "     pr.passed_review_ind  " +
                "    ) END AS passed_review_ind,  " +
                "   " +
                " p.project_status_id AS project_stat_id,  " +
                " pr.point_adjustment,  " +
                " pre.reliable_ind,  " +
                " pr.rating_order,  " +
                " NVL((  " +
                "   SELECT value  " +
                "   FROM project_info pi_dr  " +
                "   WHERE pi_dr.project_info_type_id = 30 AND pi_dr.project_id = p.project_id  " +
                "   ), (  " +
                "   SELECT value  " +
                "   FROM project_info pi_am  " +
                "   WHERE pi_am.project_info_type_id = 16 AND pi_am.project_id = p.project_id  " +
                "   )) AS amount,  " +
                " (  " +
                "  SELECT value  " +
                "  FROM project_info  " +
                "  WHERE project_id = p.project_id AND project_info_type_id = 26  " +
                "  ) AS dr_ind,  " +
                " p.project_category_id,  " +
                " CASE WHEN ppd.actual_start_time IS NOT NULL THEN ppd.actual_start_time ELSE psd.actual_start_time END AS posting_date,  " +
                " (cc.component_name || ' - ' || cv.version_text) AS project_desc,  " +
                " nvl(pwa.actual_end_time, pwa.scheduled_end_time) AS winner_announced,  " +
                " (  " +
                "  SELECT max(s.create_date) AS submission_date  " +
                "  FROM submission s,  " +
                "   upload u  " +
                "  WHERE s.submission_type_id = 1 AND s.upload_id = u.upload_id AND u.project_id = p.project_id AND u.resource_id = r.resource_id AND u.  " +
                "   upload_status_id = 1 AND u.upload_type_id = 1  " +
                "  ) AS submission_date  " +
                "FROM project_result pr,  " +
                " project p,  " +
                " project_info pi,  " +
                " comp_catalog cc,  " +
                " resource r,  " +
                " resource_info r1,  " +
                " project_info pivers,  " +
                " comp_versions cv,  " +
                " OUTER resource_info r2,  " +
                " OUTER project_phase psd,  " +
                " OUTER project_phase ppd,  " +
                " OUTER project_phase pwa,  " +
                " OUTER project_reliability pre  " +
                "WHERE p.project_id = pr.project_id AND p.project_id = pi.project_id AND pi.project_info_type_id = 2 AND r.project_id = p.project_id AND r  " +
                " .resource_role_id = 1 AND r.resource_id = r1.resource_id AND r1.resource_info_type_id = 1 AND r1.value = pr.user_id AND r.resource_id =   " +
                " r2.resource_id AND r2.resource_info_type_id = 6 AND cc.component_id = pi.value AND pivers.project_id = p.project_id AND pivers.  " +
                " project_info_type_id = 1 AND pivers.value = cv.comp_vers_id AND pwa.project_id = p.project_id AND pwa.phase_type_id IN (6, 21) AND psd  " +
                " .project_id = p.project_id AND psd.phase_type_id = 2 AND ppd.project_id = p.project_id AND ppd.phase_type_id = 1 AND pre.project_id = pr.  " +
                " project_id AND pre.user_id = pr.user_id";

        final String RESULT_INSERT =
                "insert into project_result (project_id, user_id, submit_ind, valid_submission_ind, raw_score, final_score, inquire_timestamp," +
                        " submit_timestamp, review_complete_timestamp, payment, old_rating, new_rating, old_reliability, new_reliability, placed, rating_ind, " +
                        " passed_review_ind, points_awarded, final_points, reliable_submission_ind, old_rating_id, " +
                        "new_rating_id, num_ratings, rating_order, potential_points) " +
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        final String DR_POINTS_INSERT =
                "insert into dr_points (dr_points_id, dr_points_status_id, track_id, dr_points_reference_type_id, "+
                        " dr_points_operation_id, dr_points_type_id, dr_points_desc, user_id," +
                        " amount, application_date, award_date, reference_id, is_potential) " +
                        "values (?, 1, ?, 1, 1, 1, ?, ?, ?, ?, ?, ?, ?)";

        final String DW_DATA_SELECT =
                "select sum(num_appeals) as num_appeals" +
                        " , sum(num_successful_appeals) as num_successful_appeals" +
                        " from submission_review " +
                        " where project_id = ? " +
                        " and user_id = ?";
        final String DW_DATA_UPDATE =
                "update project_result set num_appeals = ?, num_successful_appeals = ? where project_id = ? and user_id = ?";

        final String NUM_RATINGS =
                " select count(*) as count " +
                        "  , pr2.user_id " +
                        " from project_result pr2 " +
                        "  , project p1 " +
                        "  , project p2 " +
                        " where pr2.project_id = p2.project_id " +
                        " and p1.project_id = ? " +
                        " and pr2.rating_ind = 1 " +
                        " and p1.phase_id = p2.phase_id " +
                        " and (p2.rating_date < p1.rating_date " +
                        "  or (p2.rating_date = p1.rating_date and p2.project_id < p1.project_id)) " +
                        " group by pr2.user_id ";

        try {
            // EXCLUDE DR
            //Map<Integer, ContestResultCalculator> stageCalculators = getStageCalculators();

            Map<Long, Integer> dRProjects = getDRProjects();

            projectSelect = prepareStatement(PROJECTS_SELECT, SOURCE_DB);
            projectSelect.setTimestamp(1, fLastLogTime);
            projectSelect.setTimestamp(2, fLastLogTime);
            projectSelect.setTimestamp(3, fLastLogTime);
            projectSelect.setTimestamp(4, fLastLogTime);
            projectSelect.setTimestamp(5, fLastLogTime);

            resultInsert = prepareStatement(RESULT_INSERT, TARGET_DB);
            drInsert = prepareStatement(DR_POINTS_INSERT, SOURCE_DB);

            dwDataSelect = prepareStatement(DW_DATA_SELECT, TARGET_DB);
            dwDataUpdate = prepareStatement(DW_DATA_UPDATE, TARGET_DB);

            psNumRatings = prepareStatement(NUM_RATINGS, TARGET_DB);

            projects = projectSelect.executeQuery();

            while (projects.next()) {
                long start = System.currentTimeMillis();

                try {
                    StringBuffer buf = new StringBuffer(1000);
                    buf.append(RESULT_SELECT);
                    buf.append(" and p.project_id in (");

                    StringBuffer delQuery = new StringBuffer(300);
                    delQuery.append("delete from project_result where project_id in (");

                    StringBuffer delDrPointsQuery = new StringBuffer(300);
                    delDrPointsQuery.append("delete from dr_points where dr_points_reference_type_id = 1 and reference_id in (");

                    boolean projectsFound = false;
                    int numProjectsFound = 0;
                    do {
                        projectsFound = true;
                        ++numProjectsFound;
                        buf.append(projects.getLong("project_id"));
                        buf.append(",");
                        delQuery.append(projects.getLong("project_id"));
                        delQuery.append(",");
                        delDrPointsQuery.append(projects.getLong("project_id"));
                        delDrPointsQuery.append(",");
                    } while (numProjectsFound < PROJECT_RESULT_LOAD_STEP_SIZE && projects.next());
                    buf.setCharAt(buf.length() - 1, ')');
                    delQuery.setCharAt(delQuery.length() - 1, ')');
                    delDrPointsQuery.setCharAt(delDrPointsQuery.length() - 1, ')');

                    if (projectsFound) {
                        log.info("Loading next " + numProjectsFound + " projects...");

                        // EXCLUDE DR
                        // List<Track> activeTracks = getActiveTracks();

                        resultSelect = prepareStatement(buf.toString(), SOURCE_DB);

                        delete = prepareStatement(delQuery.toString(), TARGET_DB);
                        delete.executeUpdate();

                        // delete dr points for these projects.
                        deleteDrPoints = prepareStatement(delDrPointsQuery.toString(), SOURCE_DB);
                        deleteDrPoints.executeUpdate();


                        // get max dr points id
                        long drPointsId = getMaxDrPointsId();

                        int count = 0;
                        //log.debug("PROCESSING PROJECT RESULTS " + project_id);

                        projectResults = resultSelect.executeQuery();

                        HashMap<Long, Integer> ratingsMap;
                        while (projectResults.next()) {
                            long project_id = projectResults.getLong("project_id");

                            psNumRatings.clearParameters();
                            psNumRatings.setLong(1, project_id);
                            numRatings = psNumRatings.executeQuery();

                            ratingsMap = new HashMap<Long, Integer>();

                            while (numRatings.next()) {
                                ratingsMap.put(numRatings.getLong("user_id"), numRatings.getInt("count"));
                            }

                            boolean passedReview = false;
                            try {
                                passedReview = projectResults.getInt("passed_review_ind") == 1;
                            } catch (Exception e) {
                                // do nothing
                            }

                            int placed = 0;
                            try {
                                placed = projectResults.getInt("placed");
                            } catch (Exception e) {
                                // do nothing
                            }

                            int numSubmissionsPassedReview = 0;
                            try {
                                numSubmissionsPassedReview = projectResults.getInt("num_submissions_passed_review");
                            } catch (Exception e) {
                                // do nothing
                            }

                            count++;

                            double pointsAwarded = 0;
                            double potentialPoints = 0;
                            Integer stage = dRProjects.get(project_id);

                            boolean hasDR = false;

                            /*
                            if (stage != null &&
                                    (projectResults.getInt("project_stat_id") == 7 ||  // COMPLETED
                                            projectResults.getInt("project_stat_id") == 8 ||  // WINNER UNRESPONSIVE
                                            projectResults.getInt("project_stat_id") == 1) && // ACTIVE
                                    // Component Testing and RIA Build contests don't need to have the rating flag on to count
                                    // towards DR.
                                    (projectResults.getInt("rating_ind") == 1
                                            || projectResults.getInt("project_category_id") == 5
                                            || projectResults.getInt("project_category_id") == 24) &&
                                    "On".equals(projectResults.getString("dr_ind"))) {

                                hasDR = true;
                                ContestResultCalculator crc = stageCalculators.get(stage);
                                if (crc != null) {
                                    if (projectResults.getDouble("amount") < 0.01) {
                                        log.warn("Project " + project_id + " has amount=0! Please check it.");
                                    }
                                    ProjectResult pr = new ProjectResult(project_id, projectResults.getInt("project_stat_id"), projectResults.getLong("user_id"),
                                            projectResults.getDouble("final_score"), placed,
                                            0, projectResults.getDouble("amount"), numSubmissionsPassedReview, passedReview);

                                    if (projectResults.getInt("project_stat_id") == 7 || projectResults.getInt("project_stat_id") == 8) {
                                        pointsAwarded = crc.calculatePointsAwarded(pr);
                                    } else if (projectResults.getInt("valid_submission_ind") == 1) {
                                        potentialPoints = crc.calculatePotentialPoints(pr);
                                    }
                                }
                            } else if ((projectResults.getInt("project_stat_id") == 7 ||       // completed
                                    projectResults.getInt("project_stat_id") == 8 ||       // winner unresponsive
                                    projectResults.getInt("project_stat_id") == 1) &&       // active
                                    "On".equals(projectResults.getString("dr_ind")) &&      // counts towards DR
                                    projectResults.getObject("posting_date") != null &&     // has a posting date
                                    projectResults.getObject("submission_date") != null) {  // has a submission

                                hasDR = true;

                                // search for tracks where it belongs:
                                List<Track> tracks = getTracksForProject(activeTracks, projectResults.getInt("project_category_id"), projectResults.getTimestamp("posting_date"));

                                // calculate points for each track:
                                for (Track t : tracks) {
                                    if (projectResults.getDouble("amount") < 0.01) {
                                        log.warn("Project " + project_id + " has amount=0! Please check it.");
                                    }
                                    ProjectResult pr = new ProjectResult(project_id, projectResults.getInt("project_stat_id"), projectResults.getLong("user_id"),
                                            projectResults.getDouble("final_score"), placed,
                                            0, projectResults.getDouble("amount"), numSubmissionsPassedReview, passedReview);

                                    drInsert.clearParameters();
                                    if (projectResults.getInt("project_stat_id") == 7 || projectResults.getInt("project_stat_id") == 8) {
                                        pointsAwarded = t.getPointsCalculator().calculatePointsAwarded(pr);
                                        if (pointsAwarded + projectResults.getInt("point_adjustment") > 0) {
                                            drInsert.setLong(1, ++drPointsId);
                                            drInsert.setLong(2, t.getTrackId());
                                            drInsert.setString(3, "Digital Run Points won for " + projectResults.getString("project_desc"));
                                            drInsert.setLong(4, pr.getUserId());
                                            drInsert.setDouble(5, pointsAwarded + projectResults.getDouble("point_adjustment"));
                                            drInsert.setTimestamp(6, projectResults.getTimestamp("posting_date"));
                                            drInsert.setTimestamp(7, projectResults.getTimestamp("winner_announced"));
                                            drInsert.setLong(8, pr.getProjectId());
                                            drInsert.setBoolean(9, false);
                                            log.debug("Inserting DR points: " + t.getTrackId() + " - " + pr.getUserId() + " - " + pointsAwarded + " ("
                                                    + projectResults.getInt("point_adjustment") + ")");
                                            drInsert.executeUpdate();
                                        } else {
                                            log.debug("Awarded 0 points: " + t.getTrackId() + " - " + pr.getUserId() + " - " + pointsAwarded + " ("
                                                    + projectResults.getInt("point_adjustment") + ")");
                                        }
                                    } else if (projectResults.getInt("valid_submission_ind") == 1) {
                                        potentialPoints = t.getPointsCalculator().calculatePotentialPoints(pr);

                                        if (potentialPoints + projectResults.getInt("point_adjustment") > 0) {
                                            drInsert.setLong(1, ++drPointsId);
                                            drInsert.setLong(2, t.getTrackId());
                                            drInsert.setString(3, "Potential Digital Run Points for " + projectResults.getString("project_desc"));
                                            drInsert.setLong(4, pr.getUserId());
                                            drInsert.setDouble(5, potentialPoints + projectResults.getDouble("point_adjustment"));
                                            drInsert.setTimestamp(6, projectResults.getTimestamp("posting_date"));
                                            drInsert.setTimestamp(7, projectResults.getTimestamp("submission_date"));
                                            drInsert.setLong(8, pr.getProjectId());
                                            drInsert.setBoolean(9, true);
                                            log.debug("Inserting DR points: " + t.getTrackId() + " - " + pr.getUserId() + " - " + potentialPoints + " ("
                                                    + projectResults.getInt("point_adjustment") + ")");
                                            drInsert.executeUpdate();
                                        } else {
                                            log.debug("Potential 0 points: " + t.getTrackId() + " - " + pr.getUserId() + " - " + potentialPoints + " ("
                                                    + projectResults.getInt("point_adjustment") + ")");
                                        }
                                    }
                                }
                            }*/
                            resultInsert.clearParameters();

                            resultInsert.setLong(1, project_id);
                            resultInsert.setLong(2, projectResults.getLong("user_id"));
                            resultInsert.setObject(3, projectResults.getObject("submit_ind"), Types.INTEGER);
                            resultInsert.setObject(4, projectResults.getObject("valid_submission_ind"), Types.INTEGER);
                            resultInsert.setObject(5, projectResults.getObject("raw_score"), Types.DECIMAL, 2);
                            resultInsert.setObject(6, projectResults.getObject("final_score"), Types.DECIMAL, 2);
                            if (projectResults.getObject("inquire_timestamp") != null) {
                                resultInsert.setObject(7, projectResults.getObject("inquire_timestamp"), Types.TIMESTAMP);
                            } else {
                                Timestamp regDate = convertToDate(projectResults.getString("registrationd_date"));
                                if (regDate != null) {
                                    resultInsert.setTimestamp(7, regDate);
                                } else {
                                    resultInsert.setNull(7, Types.TIMESTAMP);
                                }
                            }
                            resultInsert.setObject(8, projectResults.getObject("submit_timestamp"), Types.TIMESTAMP);
                            resultInsert.setObject(9, projectResults.getObject("review_completed_timestamp"), Types.TIMESTAMP);
                            resultInsert.setObject(10, projectResults.getObject("payment"), Types.DECIMAL, 2);
                            resultInsert.setObject(11, projectResults.getObject("old_rating"), Types.INTEGER);
                            resultInsert.setObject(12, projectResults.getObject("new_rating"), Types.INTEGER);
                            resultInsert.setObject(13, projectResults.getObject("reliability_before_resolution"), Types.DECIMAL, 2);
                            resultInsert.setObject(14, projectResults.getObject("reliability_after_resolution"), Types.DECIMAL, 2);

                            Object placement = projectResults.getObject("placed");
                            Object passedReviewInd = projectResults.getObject("passed_review_ind");

                            if (placement == null && projectResults.getInt("project_category_id") == 38 &&
                                    passedReviewInd != null && projectResults.getInt("passed_review_ind") == 1) {
                                placement = new Integer(1);
                            }

                            resultInsert.setObject(15, placement, Types.INTEGER);
                            resultInsert.setObject(16, projectResults.getObject("rating_ind"), Types.INTEGER);
                            resultInsert.setObject(17, passedReviewInd, Types.INTEGER);

                            if (hasDR) {
                                resultInsert.setDouble(18, pointsAwarded);
                                resultInsert.setDouble(19, pointsAwarded + projectResults.getInt("point_adjustment"));
                            } else {
                                resultInsert.setNull(18, Types.DECIMAL);
                                resultInsert.setNull(19, Types.DECIMAL);
                            }
                            resultInsert.setInt(20, projectResults.getInt("reliable_ind"));

                            resultInsert.setInt(21, projectResults.getString("old_rating") == null ? -2 : projectResults.getInt("old_rating"));
                            resultInsert.setInt(22, projectResults.getString("new_rating") == null ? -2 : projectResults.getInt("new_rating"));
                            Long tempUserId = new Long(projectResults.getLong("user_id"));
                            int currNumRatings = 0;
                            if (ratingsMap.containsKey(tempUserId)) {
                                currNumRatings = ratingsMap.get(tempUserId);
                            }
                            resultInsert.setInt(23, projectResults.getInt("rating_ind") == 1 ? currNumRatings + 1 : currNumRatings);
                            resultInsert.setObject(24, projectResults.getObject("rating_order"), Types.INTEGER);

                            if (hasDR) {
                                resultInsert.setDouble(25, potentialPoints);
                            } else {
                                resultInsert.setNull(25, Types.DECIMAL);
                            }

                            //log.debug("before result insert");
                            try {
                                resultInsert.executeUpdate();
                            } catch(Exception e) {
                                // Notes: it seems same user will appear in resource table twice
                                log.info("project_id: " + project_id + " user_id: " + projectResults.getLong("user_id"));
                                log.info(e);
                                throw(e);
                            }
                            //log.debug("after result insert");

                            //printLoadProgress(count, "project result");

                            dwDataSelect.clearParameters();
                            dwDataSelect.setLong(1, project_id);
                            dwDataSelect.setLong(2, projectResults.getLong("user_id"));
                            dwData = dwDataSelect.executeQuery();
                            if (dwData.next()) {
                                dwDataUpdate.clearParameters();
                                if (dwData.getString("num_appeals") == null) {
                                    dwDataUpdate.setNull(1, Types.DECIMAL);
                                } else {
                                    dwDataUpdate.setInt(1, dwData.getInt("num_appeals"));
                                }
                                if (dwData.getString("num_successful_appeals") == null) {
                                    dwDataUpdate.setNull(2, Types.DECIMAL);
                                } else {
                                    dwDataUpdate.setInt(2, dwData.getInt("num_successful_appeals"));
                                }
                                dwDataUpdate.setLong(3, project_id);
                                dwDataUpdate.setLong(4, projectResults.getLong("user_id"));
                                dwDataUpdate.executeUpdate();
                            }

                        }
                        log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
                    } else {
                        log.info("loaded " + 0 + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
                    }
                } finally {
                    close(delete);
                    close(deleteDrPoints);
                    close(resultSelect);
                }
            }

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'project_result / project' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(projectResults);
            close(projects);
            close(projectSelect);
            close(resultInsert);
            close(dwDataSelect);
            close(dwDataUpdate);
            close(dwData);
        }
    }

    /**
     * <p/>
     * Gets all projects with a defined stage.
     * </p>
     *
     * @return a list containing the DR project IDs.
     * @since 1.1.0
     */
    private Map<Long, Integer> getDRProjects() throws Exception {
        PreparedStatement select = null;
        ResultSet rs = null;

        Map<Long, Integer> dRProjects = new HashMap<Long, Integer>();
        try {
            //get data from source DB
            final String SELECT = "select " +
                    "   project_id, stage_id " +
                    "from " +
                    "   project " +
                    "where " +
                    "   stage_id is not null and digital_run_ind = 1";

            select = prepareStatement(SELECT, TARGET_DB);

            rs = select.executeQuery();
            while (rs.next()) {
                dRProjects.put(rs.getLong("project_id"), rs.getInt("stage_id"));
            }

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("could not get DR projects.");
        } finally {
            close(rs);
            close(select);
        }
        return dRProjects;
    }

    /**
     * Get a map with the class for calculating points and prizes for each stage id
     *
     * @return
     * @throws Exception
     */
    private Map<Integer, ContestResultCalculator> getStageCalculators() throws Exception {
        final String SELECT =
                " select  s.stage_id, min(class_name) as class_name " +
                        " from stage s " +
                        "      , contest_stage_xref x " +
                        "      , contest c " +
                        "      , contest_result_calculator_lu calc " +
                        " where s.stage_id = x.stage_id " +
                        " and x.contest_id = c.contest_id " +
                        " and c.contest_result_calculator_id = calc.contest_result_calculator_id " +
                        " and c.contest_type_id = 19 " +
                        " group by  s.stage_id ";

        Map<Integer, ContestResultCalculator> result = new HashMap<Integer, ContestResultCalculator>();
        PreparedStatement select = null;

        try {
            select = prepareStatement(SELECT, SOURCE_DB);
            ResultSet rs = select.executeQuery();

            while (rs.next()) {
                String className = rs.getString("class_name");
                ContestResultCalculator calc = (ContestResultCalculator) Class.forName(className).newInstance();
                result.put(rs.getInt("stage_id"), calc);
            }
        } finally {
            close(select);
        }
        return result;
    }

    // private helper method to get active tracks
    private List<Track> getActiveTracks() throws Exception {
        PreparedStatement select = null;
        ResultSet rs = null;

//        log.debug("getActiveTracks ");

        List<Track> activeTracks = new ArrayList<Track>();
        try {
            //get data from source DB
            final String SELECT = "select t.track_id, tpcx.project_category_id, t.track_start_date, t.track_end_date, pcl.class_name" +
                    " from track t, track_project_category_xref tpcx, points_calculator_lu pcl" +
                    " where t.track_id = tpcx.track_id" +
                    " and t.points_calculator_id = pcl.points_calculator_id" +
                    " and t.track_status_id = 1"; // Active

            select = prepareStatement(SELECT, SOURCE_DB);

            rs = select.executeQuery();
            while (rs.next()) {
                activeTracks.add(new Track(rs.getLong("track_id"),
                        rs.getLong("project_category_id"),
                        rs.getTimestamp("track_start_date"),
                        rs.getTimestamp("track_end_date"),
                        (ContestResultCalculatorV2) Class.forName(rs.getString("class_name")).newInstance()));

//                log.debug("getActiveTracks: Add: " + rs.getLong("track_id"));
            }

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("could not get active tracks.");
        } finally {
            close(rs);
            close(select);
        }
        return activeTracks;
    }

    // private helper method to get maximum dr_points id.
    private long getMaxDrPointsId() throws Exception {
        PreparedStatement select = null;
        ResultSet rs = null;

        try {
            //get data from source DB
            final String SELECT = "select max(dr_points_id) as max_id from dr_points";

            select = prepareStatement(SELECT, SOURCE_DB);

            rs = select.executeQuery();
            if (rs.next()) {
                log.debug("getMaxDrPointsId: " + rs.getLong("max_id"));
                return rs.getLong("max_id");
            } else {
                log.debug("getMaxDrPointsId: 1000 ");
                return 1000;
            }
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("could not get max dr points id.");
        } finally {
            close(rs);
            close(select);
        }
    }

    // private helper method to get active tracks for a particular project type.
    private List<Track> getTracksForProject(List<Track> activeTracks, int projectCategoryId,
                                            Timestamp applicationDate) {

//        log.debug("getTracksForProject: " + projectCategoryId);
//        log.debug("applicationDate: " + applicationDate);

        List<Track> tracksForProject = new ArrayList<Track>();
        for (Track t : activeTracks) {

//            log.debug("t.getProjectCategoryId(): " + t.getProjectCategoryId());
//            log.debug("t.getStart(): " + t.getStart());
//            log.debug("t.getEnd(): " + t.getEnd());

            if (t.getProjectCategoryId() == projectCategoryId &&
                    t.getStart().compareTo(applicationDate) <= 0 &&
                    t.getEnd().compareTo(applicationDate) >= 0) {
                tracksForProject.add(t);
//                log.debug("getTracksForProject: Add: " + t.getTrackId());
            }
        }
        return tracksForProject;
    }

    private static class Track {
        long trackId;
        long projectCategoryId;
        Timestamp start;
        Timestamp end;
        ContestResultCalculatorV2 pointsCalculator;

        public Track(long trackId, long projectCategoryId, Timestamp start, Timestamp end, ContestResultCalculatorV2 pointsCalculator) {
            super();
            this.trackId = trackId;
            this.projectCategoryId = projectCategoryId;
            this.start = start;
            this.end = end;
            this.pointsCalculator = pointsCalculator;
        }

        protected long getTrackId() {
            return trackId;
        }

        protected void setTrackId(long trackId) {
            this.trackId = trackId;
        }

        protected long getProjectCategoryId() {
            return projectCategoryId;
        }

        protected void setProjectCategoryId(long projectCategoryId) {
            this.projectCategoryId = projectCategoryId;
        }

        protected Timestamp getStart() {
            return start;
        }

        protected void setStart(Timestamp start) {
            this.start = start;
        }

        protected Timestamp getEnd() {
            return end;
        }

        protected void setEnd(Timestamp end) {
            this.end = end;
        }

        protected ContestResultCalculatorV2 getPointsCalculator() {
            return pointsCalculator;
        }

        protected void setPointsCalculator(ContestResultCalculatorV2 pointsCalculator) {
            this.pointsCalculator = pointsCalculator;
        }

    }

}
