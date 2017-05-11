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


public class TCLoadTestcaseResponse extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadTestcaseResponse.class);

    @Override
    public void performLoad() throws Exception {
        doLoadTestcaseResponse();
    }

    /**
     * Loads testcase responses
     *
     * @throws Exception if any error occurs
     */
    private void doLoadTestcaseResponse() throws Exception {
        log.info("load testcase_response");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;

        final String SELECT =
                "select ri.scorecard_question_id" +
                        "   , r.review_id as scorecard_id" +
                        "   ,ri1.value as user_id " +
                        "   ,ri2.value as reviewer_id " +
                        "   ,u.project_id" +
                        "   ,ri.answer " +
                        "  from review_item  ri," +
                        "    review r," +
                        "    resource res," +
                        "    submission s," +
                        "    upload u," +
                        "    project p, " +
                        "    resource_info ri1," +
                        "    resource_info ri2," +
                        "    scorecard_question sq " +
                        "  where ri.review_id = r.review_id " +
                        "   and s.submission_type_id = 1 " +
                        "   and r.resource_id = res.resource_id " +
                        "   and res.resource_role_id in (4,5,6,7) " +
                        "   and r.submission_id = s.submission_id " +
                        "   and u.upload_id = s.upload_id " +
                        "   and u.project_id = p.project_id " +
                        "   and p.project_status_id <> 3 " +
                        "   and p.project_category_id in " + LOAD_CATEGORIES +
                        "   and sq.scorecard_question_id = ri.scorecard_question_id " +
                        "   and sq.scorecard_question_type_id = 3 " +
                        "   and ri1.resource_id = u.resource_id " +
                        "   and ri1.resource_info_type_id = 1 " +
                        "   and ri2.resource_id = r.resource_id " +
                        "   and ri2.resource_info_type_id = 1 " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        "   and (ri.modify_date > ? " +
                        "   OR r.modify_date > ? " +
                        "   OR res.modify_date > ? " +
                        "   OR s.modify_date > ? " +
                        "   OR u.modify_date > ? " +
                        "   OR ri1.modify_date > ? " +
                        "   OR ri2.modify_date > ? " +
                        "   OR sq.modify_date > ? " +
                        (needLoadMovedProject() ? " OR ri.modify_user <> 'Converter' " +
                                " OR r.modify_user <> 'Converter' " +
                                " OR res.modify_user <> 'Converter' " +
                                " OR s.modify_user <> 'Converter' " +
                                " OR u.modify_user <> 'Converter' " +
                                " OR ri1.modify_user <> 'Converter' " +
                                " OR ri2.modify_user <> 'Converter' " +
                                " OR sq.modify_user <> 'Converter' " +
                                ")"
                                : ")");
        final String UPDATE =
                "update testcase_response set user_id=?, reviewer_id=?, project_id=?, num_tests=?, num_passed=? where scorecard_question_id = ? and scorecard_id = ?";

        final String INSERT =
                "insert into testcase_response (user_id, reviewer_id, project_id, num_tests, num_passed, scorecard_question_id, scorecard_id) values (?, ?, ?, ?, ?, ?, ?)";


        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            select.setTimestamp(1, fLastLogTime);
            select.setTimestamp(2, fLastLogTime);
            select.setTimestamp(3, fLastLogTime);
            select.setTimestamp(4, fLastLogTime);
            select.setTimestamp(5, fLastLogTime);
            select.setTimestamp(6, fLastLogTime);
            select.setTimestamp(7, fLastLogTime);
            select.setTimestamp(8, fLastLogTime);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            int count = 0;

            rs = select.executeQuery();

            while (rs.next()) {
                update.clearParameters();

                // The answer should be like num_passed/num_tests
                String answer = rs.getString("answer");
                String[] tests = answer == null ? new String[0] : answer.split("/");
                String numTests = "1";
                String numPassed = "1";
                if (tests.length >= 2) {
                    try {
                        Integer.parseInt(tests[0]);
                        Integer.parseInt(tests[1]);
                        numPassed = tests[0];
                        numTests = tests[1];
                    } catch (Exception e) {
                        log.debug("the answer for testcase is: " + answer);
                    }
                }
                update.setObject(1, rs.getObject("user_id"));
                update.setObject(2, rs.getObject("reviewer_id"));
                update.setObject(3, rs.getObject("project_id"));
                update.setObject(4, numTests);
                update.setObject(5, numPassed);
                update.setLong(6, rs.getLong("scorecard_question_id"));
                update.setLong(7, rs.getLong("scorecard_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();

                    insert.setObject(1, rs.getObject("user_id"));
                    insert.setObject(2, rs.getObject("reviewer_id"));
                    insert.setObject(3, rs.getObject("project_id"));
                    insert.setObject(4, numTests);
                    insert.setObject(5, numTests);
                    insert.setLong(6, rs.getLong("scorecard_question_id"));
                    insert.setLong(7, rs.getLong("scorecard_id"));

                    insert.executeUpdate();
                }
                count++;

            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'testcase_response' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(update);
            close(select);
        }
    }

}
