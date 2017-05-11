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


public class TCLoadTestcaseAppeal extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadTestcaseAppeal.class);

    @Override
    public void performLoad() throws Exception {
        doLoadTestcaseAppeal();
    }

    /**
     * Loads testcase appeals
     *
     * @throws Exception if any error occurs
     */
    private void doLoadTestcaseAppeal() throws Exception {
        log.info("load Testcase Appeal");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        ResultSet projects;
        PreparedStatement projectSelect;

        final String SELECT =
                "select ric.review_item_comment_id as appeal_id " +
                        "   ,ri.scorecard_question_id  " +
                        "   ,r.review_id as scorecard_id " +
                        "   ,ri1.value as user_id " +
                        "   ,ri2.value as reviewer_id " +
                        "   ,u.project_id " +
                        "   ,ri.answer " +
                        "   ,ric.content as appeal_text " +
                        "   ,ric.extra_info as successful_ind " +
                        "   ,ric_resp.content as appeal_response " +
                        "   ,ric_resp.extra_info as raw_answer " +
                        "from review_item_comment ric,  " +
                        "   review_item  ri, " +
                        "   review r, " +
                        "   submission s, " +
                        "   upload u, " +
                        "   project p, " +
                        "   resource res, " +
                        "   resource_info ri1," +
                        "   resource_info ri2," +
                        "   scorecard_question sq, " +
                        "   review_item_comment ric_resp " +
                        "where ric.review_item_id = ri.review_item_id " +
                        "   and s.submission_type_id = 1 " +
                        "   and ri.review_id = r.review_id " +
                        "   and r.submission_id = s.submission_id " +
                        "   and u.upload_id = s.upload_id " +
                        "   and u.project_id = p.project_id " +
                        "   and p.project_status_id <> 3 " +
                        "   and p.project_category_id in " + LOAD_CATEGORIES +
                        "   and r.resource_id = res.resource_id " +
                        "   and res.resource_role_id in (4, 5, 6, 7) " +
                        "   and ri.scorecard_question_id = sq.scorecard_question_id " +
                        "   and sq.scorecard_question_type_id = 3 " +
                        "   and ric_resp.review_item_id = ri.review_item_id " +
                        "   and ric_resp.comment_type_id = 5 " +
                        "   and ric.comment_type_id = 4 " +
                        "   and ri1.resource_id = u.resource_id " +
                        "   and ri1.resource_info_type_id = 1 " +
                        "   and ri2.resource_id = r.resource_id " +
                        "   and ri2.resource_info_type_id = 1 " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        "   and u.project_id in (select distinct project_id from project_result) " +
                        "   and (ric.modify_date > ? " +
                        "   OR ri.modify_date > ? " +
                        "   OR r.modify_date > ?" +
                        "   OR s.modify_date > ?" +
                        "   OR u.modify_date > ? " +
                        "   OR res.modify_date > ? " +
                        "   OR sq.modify_date > ? " +
                        "   OR ri1.modify_date > ? " +
                        "   OR ri2.modify_date > ? " +
                        "   OR ric_resp.modify_date > ? " +
                        (needLoadMovedProject() ? " OR ric.modify_user <> 'Converter' " +
                                " OR ri.modify_user <> 'Converter' " +
                                " OR r.modify_user <> 'Converter' " +
                                " OR s.modify_user <> 'Converter' " +
                                " OR u.modify_user <> 'Converter' " +
                                " OR res.modify_user <> 'Converter' " +
                                " OR sq.modify_user <> 'Converter' " +
                                " OR ri1.modify_user <> 'Converter' " +
                                " OR ri2.modify_user <> 'Converter' " +
                                " OR ric_resp.modify_user <> 'Converter' " +
                                ")"
                                : ")");

        final String UPDATE =
                "update testcase_appeal set scorecard_question_id = ?, scorecard_id = ?, user_id=?, reviewer_id=?, project_id=?, " +
                        "raw_num_passed=?, raw_num_tests=?, final_num_passed=?, final_num_tests=?, appeal_text=?, appeal_response=?, successful_ind = ? where appeal_id=?";

        final String INSERT =
                "insert into testcase_appeal (scorecard_question_id, scorecard_id, user_id, reviewer_id, project_id, " +
                        "raw_num_passed, raw_num_tests, final_num_passed, final_num_tests, appeal_text, appeal_response, appeal_id, successful_ind) " +
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            select.clearParameters();
            select.setTimestamp(1, fLastLogTime);
            select.setTimestamp(2, fLastLogTime);
            select.setTimestamp(3, fLastLogTime);
            select.setTimestamp(4, fLastLogTime);
            select.setTimestamp(5, fLastLogTime);
            select.setTimestamp(6, fLastLogTime);
            select.setTimestamp(7, fLastLogTime);
            select.setTimestamp(8, fLastLogTime);
            select.setTimestamp(9, fLastLogTime);
            select.setTimestamp(10, fLastLogTime);

            rs = select.executeQuery();

            int count = 0;
            while (rs.next()) {
                update.clearParameters();

                update.setLong(1, rs.getLong("scorecard_question_id"));
                update.setLong(2, rs.getLong("scorecard_id"));
                update.setObject(3, rs.getObject("user_id"), Types.INTEGER);
                update.setObject(4, rs.getObject("reviewer_id"), Types.INTEGER);
                update.setObject(5, rs.getObject("project_id"), Types.INTEGER);

                String answer = rs.getString("raw_answer");
                String[] tests = answer == null ? new String[0] : answer.split("/");
                String rawNumTests = "1";
                String rawNumPassed = "1";
                if (tests.length >= 2) {
                    rawNumPassed = tests[0];
                    rawNumTests = tests[1];
                }

                update.setObject(6, rawNumPassed);
                update.setObject(7, rawNumTests);

                answer = rs.getString("answer");
                tests = answer == null ? new String[0] : answer.split("/");
                String finalNumTests = "1";
                String finalNumPassed = "1";
                if (tests.length >= 2) {
                    finalNumPassed = tests[0];
                    finalNumTests = tests[1];
                }

                update.setObject(8, finalNumPassed);
                update.setObject(9, finalNumTests);
                update.setObject(10, rs.getObject("appeal_text"));
                update.setObject(11, rs.getObject("appeal_response"));
                String successfulInd = rs.getString("successful_ind");
                if (successfulInd == null) {
                    update.setNull(12, Types.INTEGER);
                } else {
                    if ("Succeeded".equals(successfulInd)) {
                        update.setInt(12, 1);
                    } else {
                        update.setInt(12, 0);
                    }
                }

                update.setLong(13, rs.getLong("appeal_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();

                    insert.setLong(1, rs.getLong("scorecard_question_id"));
                    insert.setLong(2, rs.getLong("scorecard_id"));
                    insert.setObject(3, rs.getObject("user_id"), Types.INTEGER);
                    insert.setObject(4, rs.getObject("reviewer_id"), Types.INTEGER);
                    insert.setObject(5, rs.getObject("project_id"), Types.INTEGER);
                    insert.setObject(6, rawNumPassed);
                    insert.setObject(7, rawNumTests);
                    insert.setObject(8, finalNumPassed);
                    insert.setObject(9, finalNumTests);
                    insert.setObject(10, rs.getObject("appeal_text"));
                    insert.setObject(11, rs.getObject("appeal_response"));
                    insert.setLong(12, rs.getLong("appeal_id"));
                    if (successfulInd == null) {
                        insert.setNull(13, Types.INTEGER);
                    } else {
                        if ("Succeeded".equals(successfulInd)) {
                            insert.setInt(13, 1);
                        } else {
                            insert.setInt(13, 0);
                        }
                    }

                    insert.executeUpdate();
                }

                count++;
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'testcase_appeal' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(update);
            close(select);
        }
    }

}
