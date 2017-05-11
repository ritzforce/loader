/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;
import com.topcoder.utilities.dwload.contestresult.ContestResult;
import com.topcoder.utilities.dwload.contestresult.ContestResultCalculator;
import com.topcoder.utilities.dwload.contestresult.ProjectResult;
import com.topcoder.utilities.dwload.contestresult.TopPerformersCalculator;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class TCLoadStageResults extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadStageResults.class);

    @Override
    public void performLoad() throws Exception {
        doLoadStageResults();
    }

    /**
     * Load the contest_result table for the contests belonging to stages whose results were modified.
     *
     * @throws Exception if any error occurs
     */
    private void doLoadStageResults() throws Exception {
        log.debug("load stage results");

        final String SELECT_STAGES =
                " select distinct s.season_id, s.stage_id, s.start_date, s.end_date " +
                        " from project_result pr, " +
                        "      stage s, " +
                        "      project p  " +
                        " where p.project_id = pr.project_id  " +
                        " and p.project_status_id <> 3  " +
                        " and p.project_category_id in " + LOAD_CATEGORIES +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        " and (p.modify_date > ? " +
                        "     OR pr.modify_date > ?) " +
                        " and ( " +
                        " select NVL(ppd.actual_start_time, psd.actual_start_time)  " +
                        " from project p " +
                        "     , OUTER project_phase psd " +
                        "     , OUTER project_phase ppd " +
                        " where  psd.project_id = p.project_id  " +
                        " and psd.phase_type_id = 2  " +
                        " and ppd.project_id = p.project_id  " +
                        " and ppd.phase_type_id = 1  " +
                        " and p.project_id = pr.project_id) between s.start_date and s.end_date ";

        final String SELECT_CONTESTS =
                " select c.contest_id, c.project_category_id, c.contest_type_id, crc.class_name, x.top_performers_factor " +
                        " from contest_stage_xref x " +
                        " ,contest c " +
                        " ,contest_result_calculator_lu crc " +
                        " where c.contest_id = x.contest_id " +
                        " and c.contest_result_calculator_id = crc.contest_result_calculator_id  " +
                        " and x.stage_id = ? ";


        PreparedStatement selectStages = null;
        PreparedStatement selectContests = null;
        ResultSet rsStages = null;
        ResultSet rsContests = null;

        try {
            selectStages = prepareStatement(SELECT_STAGES, SOURCE_DB);
            selectContests = prepareStatement(SELECT_CONTESTS, SOURCE_DB);

            selectStages.setTimestamp(1, fLastLogTime);
            selectStages.setTimestamp(2, fLastLogTime);

            rsStages = selectStages.executeQuery();

            while (rsStages.next()) {
                selectContests.clearParameters();
                selectContests.setInt(1, rsStages.getInt("stage_id"));
                rsContests = selectContests.executeQuery();

                Timestamp startDate = rsStages.getTimestamp("start_date");
                Timestamp endDate = rsStages.getTimestamp("end_date");
                int seasonId = rsStages.getInt("season_id");

                while (rsContests.next()) {
                    loadDRContestResults(seasonId, startDate, endDate, rsContests.getInt("project_category_id"), rsContests.getInt("contest_id"),
                            rsContests.getString("class_name"), rsContests.getDouble("top_performers_factor"));
                }

            }

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'contest_result' table for stages failed.\n" +
                    sqle.getMessage());
        } finally {
            close(selectStages);
            close(rsStages);
        }

    }

    /**
     * Helper method to load contest results for the specified contest.
     *
     * @param seasonId the season id
     * @param startDate the start date
     * @param endDate the end date
     * @param projectCategoryId the project category id
     * @param contestId the contest id
     * @param className the class name
     * @param factor the factor
     *
     * @throws Exception if any error occurs
     */
    private void loadDRContestResults(int seasonId, Timestamp startDate, Timestamp endDate, int projectCategoryId,
                                      int contestId, String className, double factor) throws Exception {

        log.debug("loading contest_result for dr contest_id=" + contestId + ", project category=" + projectCategoryId + " from " + startDate + " to " + endDate);
        final String SELECT_RESULTS =
                " select p.project_id " +
                        "       ,p.project_status_id " +
                        "       ,pr.user_id " +
                        "       ,pr.placed " +
                        "       ,pr.point_adjustment " +
                        "       ,pr.final_score " +
                        "       ,pr.passed_review_ind " +
                        "       , NVL((select value from project_info pi_dr where pi_dr.project_info_type_id = 30 and pi_dr.project_id = p.project_id), " +
                        "          (select value from project_info pi_am where pi_am.project_info_type_id = 16 and pi_am.project_id = p.project_id)) as amount " +
                        "       ,(select count(*) from submission s, upload u  " +
                        "         where s.submission_type_id = 1 and u.upload_id = s.upload_id and project_id = p.project_id  " +
                        "         and submission_status_id in (1, 4) " +
                        "        ) as num_submissions_passed_review  " +
                        "    ,case when exists(select '1' from submission s,upload u,resource r, resource_info ri " +
                        "           where s.submission_type_id = 1 and r.resource_id = ri.resource_id and ri.resource_info_type_id = 1 and u.resource_id = r.resource_id " +
                        "           and u.upload_id = s.upload_id and u.project_id = pr.project_id and ri.value = pr.user_id and submission_status_id in (1,2,3,4)) then pr.valid_submission_ind  " +
                        "    else 0 end as valid_submission_ind " +
                        " from project p " +
                        "    ,project_result pr " +
                        "    ,project_info pi_dr " +
                        " where pi_dr.project_id = p.project_id " +
                        " and pi_dr.project_info_type_id = 26 " +
                        " and pi_dr.value = 'On' " +
                        " and p.project_id = pr.project_id " +
                        // component testing doesn't need to check for rating
                        " and (pr.rating_ind=1 or p.project_category_id = 5)" +
                        // for development board, load development and component testing
                        " and p.project_category_id in (" + ((projectCategoryId == 2) ? "2, 5" : String.valueOf(projectCategoryId)) + ") " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        " and ( " +
                        "      select NVL(ppd.actual_start_time, psd.actual_start_time)  " +
                        "      from project p1 " +
                        "        , OUTER project_phase psd " +
                        "        , OUTER project_phase ppd " +
                        "        where  psd.project_id = p1.project_id  " +
                        "        and psd.phase_type_id = 2  " +
                        "        and ppd.project_id = p1.project_id  " +
                        "        and ppd.phase_type_id = 1  " +
                        "        and p1.project_id = p.project_id) between ? and ? ";


        final String INSERT = "insert into contest_result(contest_id, coder_id, initial_points, final_points, potential_points, current_place, current_prize) " +
                " values(?,?,?,?,?,?,?)";

        ResultSet rs = null;
        PreparedStatement selectResults = null;
        PreparedStatement insert = null;

        try {
            selectResults = prepareStatement(SELECT_RESULTS, SOURCE_DB);
            // for dev contests, load also component testing projects results
            selectResults.setTimestamp(1, startDate);
            selectResults.setTimestamp(2, endDate);

            insert = prepareStatement(INSERT, TARGET_DB);

            ContestResultCalculator calc = (ContestResultCalculator) Class.forName(className).newInstance();
            if (calc instanceof TopPerformersCalculator) {
                ((TopPerformersCalculator) calc).setFactor(factor);
            }

            rs = selectResults.executeQuery();

            List<ProjectResult> pr = new ArrayList<ProjectResult>();
            int count = 0;
            while (rs.next()) {
                if (rs.getDouble("amount") < 0.01) {
                    log.warn("Project: " + rs.getLong("project_id") + " has zero amount!");
                }

                if (rs.getInt("valid_submission_ind") == 1 || rs.getInt("project_status_id") == 7) {
                    ProjectResult res = new ProjectResult(rs.getLong("project_id"), rs.getInt("project_status_id"), rs.getLong("user_id"),
                            rs.getDouble("final_score"), rs.getInt("placed"), rs.getInt("point_adjustment"), rs.getDouble("amount"),
                            rs.getInt("num_submissions_passed_review"), rs.getBoolean("passed_review_ind"));

                    pr.add(res);
                }
                count++;
            }
            close(rs);
            log.debug("    " + count + " projects processed for the contest");

            simpleDelete("contest_result", "contest_id", contestId);

            List<ContestResult> results = calc.calculateResults(pr, getContestPrizesAmount(contestId));

            count = 0;

            for (ContestResult result : results) {
                insert.clearParameters();
                insert.setInt(1, contestId);
                insert.setLong(2, result.getCoderId());
                insert.setDouble(3, result.getInitialPoints());
                insert.setDouble(4, result.getFinalPoints());
                insert.setDouble(5, result.getPotentialPoints());
                insert.setInt(6, result.getPlace());

                if (result.getPrize() != null) {
                    insert.setDouble(7, result.getPrize());
                } else {
                    insert.setNull(7, Types.DOUBLE);
                }

                insert.executeUpdate();

                count++;
            }

            log.debug(count + " results added for contest " + contestId);
        } finally {
            close(selectResults);
            close(insert);
        }

    }

    /**
     * Get the prizes for the specified contest.
     *
     * @param contestId
     * @return
     * @throws Exception
     */
    private List<Double> getContestPrizesAmount(long contestId) throws Exception {
        final String SELECT =
                "select  place, prize_amount " +
                        "from contest_prize  " +
                        "where contest_id = ? " +
                        "order by place";

        List<Double> prizes = new ArrayList<Double>();
        PreparedStatement select = null;

        try {
            select = prepareStatement(SELECT, SOURCE_DB);
            select.setLong(1, contestId);
            ResultSet rs = select.executeQuery();

            int expectedPlace = 1;
            while (rs.next()) {
                if (rs.getString("place") != null && rs.getInt("place") != expectedPlace) {
                    throw new Exception("Error in prizes for contest " + contestId + " expected a prize for place " + expectedPlace);
                }

                prizes.add(rs.getDouble("prize_amount"));
                expectedPlace++;
            }
        } finally {
            close(select);
        }
        return prizes;
    }

}
