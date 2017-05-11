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
import java.sql.Types;


public class TCLoadDesignProjectResults extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadDesignProjectResults.class);

    /**
     * The submission type id of checkpoint submission.
     *
     * @since 1.2.4
     */
    private static final int CHECKPOINT_SUBMISSION_TYPE_ID = 3;

    /**
     * The DR percentage table of the studio contests.
     *
     * @since 1.2.4
     */
    private static final double[][] STUDIO_DR_PERCENTAGE_TABLE = new double[][]{
            new double[]{1, 0, 0, 0, 0},
            new double[]{0.7, 0.3, 0, 0, 0},
            new double[]{0.65, 0.25, 0.1, 0, 0},
            new double[]{0.6, 0.22, 0.1, 0.08, 0},
            new double[]{0.56, 0.2, 0.1, 0.08, 0.06}
    };


    @Override
    public void performLoad() throws Exception {
        doLoadDesignProjectResults();
    }

    /**
     * Loads design project result
     *
     * @throws Exception if any error.
     *
     * @since 1.2.4
     */
    public void doLoadDesignProjectResults() throws Exception {
        log.info("load design project results");

        PreparedStatement firstTimeSelect = null;
        PreparedStatement projectSelect = null;
        PreparedStatement resultSelect = null;
        PreparedStatement resultInsert = null;
        PreparedStatement delete = null;

        ResultSet rs = null;
        ResultSet projects = null;
        ResultSet projectResults = null;


        try {

            firstTimeSelect = prepareStatement("SELECT count(*) from design_project_result", TARGET_DB);
            rs = firstTimeSelect.executeQuery();
            rs.next();

            // no records, it's the first run of loading design project result
            boolean firstRun = rs.getInt(1) == 0;

            final String PROJECTS_SELECT =
                    "select distinct p.project_id " +
                            "from project p, " +
                            "project_info pi, " +
                            "comp_versions cv, " +
                            "comp_catalog cc, " +
                            "project_category_lu pcl " +
                            "where " +
                            " p.project_id = pi.project_id " +
                            " and p.project_category_id = pcl.project_category_id " +
                            " and pcl.project_type_id = 3 " +
                            "and p.project_status_id NOT IN (1, 2, 3, 9, 10, 11)" +
                            "and pi.project_info_type_id = 1 " +
                            "and cv.comp_vers_id= pi.value " +
                            "and cc.component_id = cv.component_id " +
                            ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                            (!firstRun ?
                                    ("and (p.modify_date > ? " +
                                            "   OR cv.modify_date > ? " +
                                            "   OR pi.modify_date > ? " +
                                            "   OR cc.modify_date > ? " +
                                            (needLoadMovedProject() ? " OR p.modify_user <> 'Converter' " +
                                                    " OR pi.modify_user <> 'Converter' " +
                                                    ")"
                                                    : ")")) : "");

            final String RESULT_SELECT = "SELECT  pj.project_id       , " +
                    "        s.submission_id    , " +
                    "        s.submission_type_id, " +
                    "        s.submission_status_id, " +
                    "        u.upload_id        , " +
                    "        r.create_date as inquire_date, " +
                    "        u.create_date as submit_date, " +
                    "        r.user_id          , " +
                    "        p.prize_id         , " +
                    "        p.prize_amount     , " +
                    "        p.prize_type_id    , " +
                    "        p.place            , " +
                    "        s.placement        , " +
                    "        s.mark_for_purchase, " +
                    "        (SELECT MAX(rev.modify_date) " +
                    "        FROM    review rev " +
                    "        WHERE   rev.submission_id = s.submission_id " +
                    "            AND rev.committed     = 1 " +
                    "        ) AS review_date, " +
                    "        (SELECT NVL(pi30.value::DECIMAL(10,2), 0) " +
                    "        FROM    project_info pi30, " +
                    "                project_info pi26 " +
                    "        WHERE   pi30.project_id           = pj.project_id " +
                    "            AND pi30.project_info_type_id = 30 " +
                    "            AND pi26.project_id           = pj.project_id " +
                    "            AND pi26.project_info_type_id = 26 " +
                    "            AND pi26.value                = 'On' " +
                    "        ) AS total_dr_points, " +
                    "        (SELECT COUNT(submission_id) " +
                    "        FROM    submission sub, " +
                    "                prize pri     , " +
                    "                upload upl " +
                    "        WHERE   upl.project_id        = pj.project_id " +
                    "            AND sub.upload_id         = upl.upload_id " +
                    "            AND sub.prize_id          = pri.prize_id " +
                    "            AND sub.submission_type_id = 1 " +
                    "            AND (sub.mark_for_purchase = 'f' OR sub.mark_for_purchase is NULL) " +
                    "        ) AS total_placements " +
                    "FROM    project pj " +
                    "INNER JOIN resource r ON r.project_id = pj.project_id and r.resource_role_id = 1 " +
                    "LEFT OUTER JOIN upload u ON pj.project_id = u.project_id and u.resource_id = r.resource_id and u.upload_status_id = 1 and u.upload_type_id = 1 " +
                    "LEFT OUTER JOIN submission s ON s.upload_id = u.upload_id and s.submission_status_id != 5 and s.submission_type_id in (1, 3) " +
                    "LEFT OUTER JOIN prize p ON s.prize_id = p.prize_id ";

            final String RESULT_INSERT =
                    "INSERT INTO design_project_result (project_id, user_id, submission_id, upload_id, prize_id, prize_amount, placement, dr_points, is_checkpoint, client_selection, submit_timestamp, review_complete_timestamp, inquire_timestamp, submit_ind, valid_submission_ind) " +
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";



            projectSelect = prepareStatement(PROJECTS_SELECT, SOURCE_DB);
            resultInsert = prepareStatement(RESULT_INSERT, TARGET_DB);


            if (!firstRun) {
                projectSelect.setTimestamp(1, fLastLogTime);
                projectSelect.setTimestamp(2, fLastLogTime);
                projectSelect.setTimestamp(3, fLastLogTime);
                projectSelect.setTimestamp(4, fLastLogTime);
            }

            projects = projectSelect.executeQuery();


            while(projects.next()) {

                long start = System.currentTimeMillis();

                try {
                    StringBuffer buf = new StringBuffer(1000);
                    buf.append(RESULT_SELECT);
                    buf.append(" WHERE pj.project_id in (");


                    StringBuffer delQuery = new StringBuffer(300);
                    delQuery.append("delete from design_project_result where project_id in (");


                    boolean projectsFound = false;
                    int numProjectsFound = 0;
                    do {
                        projectsFound = true;
                        ++numProjectsFound;
                        buf.append(projects.getLong("project_id"));
                        buf.append(",");
                        delQuery.append(projects.getLong("project_id"));
                        delQuery.append(",");
                    } while (numProjectsFound < PROJECT_RESULT_LOAD_STEP_SIZE && projects.next());


                    buf.setCharAt(buf.length() - 1, ')');
                    delQuery.setCharAt(delQuery.length() - 1, ')');


                    if(projectsFound) {
                        log.info("Loading results of next " + numProjectsFound + " design projects...");

                        resultSelect = prepareStatement(buf.toString(), SOURCE_DB);

                        delete = prepareStatement(delQuery.toString(), TARGET_DB);
                        delete.executeUpdate();


                        int count = 0;

                        projectResults = resultSelect.executeQuery();

                        while(projectResults.next()) {

                            int index = 0;
                            resultInsert.clearParameters();

                            if (projectResults.getObject("submission_id") != null) { // if submitted
                                boolean isCheckPointSubmission = (projectResults.getLong("submission_type_id") ==
                                        CHECKPOINT_SUBMISSION_TYPE_ID);
                                boolean markForPurchase = projectResults.getBoolean("mark_for_purchase");
                                int placement = projectResults.getInt("placement");
                                int totalPlacement = projectResults.getInt("total_placements");
                                double totalDRPoints = 0;

                                if(projectResults.getObject("total_dr_points") != null) {
                                    totalDRPoints = projectResults.getDouble("total_dr_points");
                                }

                                Double userDRPoints = null;

                                if (!markForPurchase && !isCheckPointSubmission) {
                                    userDRPoints =
                                            getDesignContestUserDRPoints(totalDRPoints, totalPlacement,
                                                    placement);
                                }

                                resultInsert.setLong(++index, projectResults.getLong("project_id"));
                                resultInsert.setLong(++index, projectResults.getLong("user_id"));
                                resultInsert.setLong(++index, projectResults.getLong("submission_id"));
                                resultInsert.setLong(++index, projectResults.getLong("upload_id"));

                                if(projectResults.getObject("prize_id") != null) {
                                    resultInsert.setLong(++index, projectResults.getLong("prize_id"));
                                } else {
                                    resultInsert.setNull(++index, Types.DECIMAL);
                                }

                                if(projectResults.getObject("prize_amount") != null) {
                                    resultInsert.setDouble(++index, projectResults.getDouble("prize_amount"));
                                } else {
                                    resultInsert.setNull(++index, Types.DECIMAL);
                                }

                                if(!isCheckPointSubmission && !markForPurchase) {
                                    // checkpoint submission and client extra purchase does not count placement
                                    resultInsert.setLong(++index, placement);
                                } else {
                                    resultInsert.setNull(++index, Types.DECIMAL);
                                }

                                if(userDRPoints != null && projectResults.getObject("prize_id") != null) {
                                    resultInsert.setDouble(++index, userDRPoints);
                                } else {
                                    resultInsert.setNull(++index, Types.DECIMAL);
                                }

                                resultInsert.setInt(++index, isCheckPointSubmission ? 1 : 0);
                                resultInsert.setInt(++index, markForPurchase ? 1 : 0);

                                resultInsert.setTimestamp(++index, projectResults.getTimestamp("submit_date"));
                                resultInsert.setTimestamp(++index, projectResults.getTimestamp("review_date"));
                                resultInsert.setTimestamp(++index, projectResults.getTimestamp("inquire_date"));
                                resultInsert.setLong(++index, 1);

                                long submission_status_id = projectResults.getLong("submission_status_id");
                                long submission_valid_ind = (submission_status_id == 1 || submission_status_id == 4) ? 1 : 0;
                                resultInsert.setLong(++index, submission_valid_ind);
                            } else { // if not submitted

                                if(projectResults.getObject("upload_id") != null ||
                                        designProjectResultExists(projectResults.getLong("project_id"), projectResults.getLong("user_id"), 0l))
                                    continue;

                                resultInsert.setLong(++index, projectResults.getLong("project_id"));
                                resultInsert.setLong(++index, projectResults.getLong("user_id"));
                                resultInsert.setLong(++index, 0);
                                resultInsert.setNull(++index, Types.DECIMAL);
                                resultInsert.setNull(++index, Types.DECIMAL);
                                resultInsert.setNull(++index, Types.DECIMAL);
                                resultInsert.setNull(++index, Types.DECIMAL);
                                resultInsert.setNull(++index, Types.FLOAT);

                                resultInsert.setNull(++index, Types.INTEGER);
                                resultInsert.setNull(++index, Types.INTEGER);

                                resultInsert.setNull(++index, Types.TIMESTAMP);
                                resultInsert.setNull(++index, Types.TIMESTAMP);

                                resultInsert.setTimestamp(++index, projectResults.getTimestamp("inquire_date"));
                                resultInsert.setLong(++index, 0);
                                resultInsert.setLong(++index, 0);
                            }

                            resultInsert.executeUpdate();
                            count++;
                        }

                        log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
                    }

                }  finally {
                    close(delete);
                    close(resultSelect);
                }
            }


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'design_project_result' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(projects);
            close(projectResults);
            close(firstTimeSelect);
            close(projectSelect);
            close(resultInsert);
        }

    }

    /**
     * Helper method to calculate the DR points for each design project result
     *
     * @param totalDRPoints the total DR points.
     * @param totalPlacements the number of placement submission
     * @param userPlacement the placement to calculation
     * @return the calcualted DR points.
     * @since 1.2.4
     */
    private static double getDesignContestUserDRPoints(double totalDRPoints, int totalPlacements, int userPlacement) {
        if(totalDRPoints <= 0 || userPlacement > 5 || userPlacement <= 0 || totalPlacements <= 0) {
            return 0;
        }

        if(totalPlacements > 5) {
            totalPlacements = 5;
        }

        return Math.round(totalDRPoints * STUDIO_DR_PERCENTAGE_TABLE[totalPlacements - 1][userPlacement - 1] * 100.0) / 100.0;
    }

    /**
     * Verifies if a design project result record of a given project id, user id, and submisison id already exists in the database
     * @param projectId Id of the project
     * @param userId Id of the user
     * @param submissionId Id of the submission
     * @return true if a design project result already exists, false otherwise
     */
    private boolean designProjectResultExists(Long projectId, Long userId, Long submissionId) throws SQLException {
        boolean exists = false;
        PreparedStatement resultQuery = null;
        ResultSet result = null;

        try {
            resultQuery = prepareStatement("select count(*) ct from design_project_result where project_id = ? and user_id = ? and submission_id = ?", TARGET_DB);
            resultQuery.setLong(1, projectId);
            resultQuery.setLong(2, userId);
            resultQuery.setLong(3, submissionId);

            result = resultQuery.executeQuery();

            result.next();

            exists = result.getLong("ct") != 0;
        } finally {
            close(result);
            close(resultQuery);
        }

        return exists;
    }

}
