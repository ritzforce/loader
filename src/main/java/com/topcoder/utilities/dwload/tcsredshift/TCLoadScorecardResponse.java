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


public class TCLoadScorecardResponse extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadScorecardResponse.class);

    @Override
    public void performLoad() throws Exception {
        doLoadScorecardResponse();
    }

    /**
     * Loads scorecard responses
     *
     * @throws Exception if any error occurs
     */
    private void doLoadScorecardResponse() throws Exception {
        log.info("load scorecard_response");
        ResultSet rs = null;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        ResultSet projects = null;
        PreparedStatement projectSelect;


        final String SELECT =
                "select ri.scorecard_question_id , " +
                        "    r.review_id as scorecard_id,   " +
                        "    ri1.value as user_id, " +
                        "    ri2.value as reviewer_id, " +
                        "    u.project_id,   " +
                        "    ri.answer answer," +
                        "    sq.scorecard_question_type_id  " +
                        "    from review_item  ri," +
                        "       review r," +
                        "       resource res," +
                        "       resource_info ri1," +
                        "       resource_info ri2," +
                        "       submission s," +
                        "       upload u," +
                        "       project p, " +
                        "       scorecard_question sq" +
                        "    where  ri.scorecard_question_id = sq.scorecard_question_id " +
                        "   and s.submission_type_id = 1 " +
                        "   and ri.review_id = r.review_id " +
                        "   and r.resource_id = res.resource_id " +
                        "   and res.resource_role_id in (2,3,4,5,6,7) " +
                        "   and ri1.resource_id = u.resource_id " +
                        "   and ri1.resource_info_type_id = 1 " +
                        "   and ri2.resource_id = r.resource_id " +
                        "   and ri2.resource_info_type_id = 1 " +
                        "   and r.submission_id = s.submission_id " +
                        "   and u.upload_id = s.upload_id " +
                        "   and u.project_id = p.project_id " +
                        "   and p.project_status_id <> 3 " +
                        "   and p.project_category_id in " + LOAD_CATEGORIES +
                        "   and sq.scorecard_question_type_id in (1,2,4) " +
                        "   and answer <> '' " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        "   and  u.project_id = ?  " +
                        "   and (ri.modify_date > ? " +
                        "   OR r.modify_date > ? " +
                        "   OR res.modify_date > ? " +
                        "   OR u.modify_date > ? " +
                        "   OR ri1.modify_date > ? " +
                        "   OR ri2.modify_date > ? " +
                        "   OR s.modify_date > ? " +
                        (needLoadMovedProject() ? " OR ri.modify_user <> 'Converter' " +
                                " OR r.modify_user <> 'Converter' " +
                                " OR res.modify_user <> 'Converter' " +
                                " OR u.modify_user <> 'Converter' " +
                                " OR ri1.modify_user <> 'Converter' " +
                                " OR ri2.modify_user <> 'Converter' " +
                                " OR s.modify_user <> 'Converter' " +
                                ")"
                                : ")");

        final String UPDATE =
                "update scorecard_response set user_id=?, reviewer_id=?, project_id=?, evaluation_id=? where scorecard_question_id = ? and scorecard_id = ?";

        final String INSERT =
                "insert into scorecard_response (user_id, reviewer_id, project_id, evaluation_id, scorecard_question_id, scorecard_id) values (?, ?, ?, ?, ?, ?)";

        long questionId = 0;
        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            projectSelect = prepareStatement(PROJECT_SELECT, SOURCE_DB);

            projects = projectSelect.executeQuery();

            int count = 0;

            while (projects.next()) {
                select.clearParameters();
                select.setLong(1, projects.getLong("project_id"));
                select.setTimestamp(2, fLastLogTime);
                select.setTimestamp(3, fLastLogTime);
                select.setTimestamp(4, fLastLogTime);
                select.setTimestamp(5, fLastLogTime);
                select.setTimestamp(6, fLastLogTime);
                select.setTimestamp(7, fLastLogTime);
                select.setTimestamp(8, fLastLogTime);

                rs = select.executeQuery();

                while (rs.next()) {
                    update.clearParameters();
                    questionId = rs.getLong("scorecard_question_id");

                    String answer = rs.getString("answer");
                    int evaluationId = getEvaluationId(rs.getInt("scorecard_question_type_id"), answer);

                    update.setObject(1, rs.getObject("user_id"));
                    update.setObject(2, rs.getObject("reviewer_id"));
                    update.setObject(3, rs.getObject("project_id"));
                    if (evaluationId != 0) {
                        update.setInt(4, evaluationId);
                    } else {
                        update.setNull(4, Types.INTEGER);
                    }
                    update.setLong(5, questionId);
                    update.setLong(6, rs.getLong("scorecard_id"));

                    int retVal = update.executeUpdate();

                    if (retVal == 0) {
                        insert.clearParameters();

                        insert.setObject(1, rs.getObject("user_id"));
                        insert.setObject(2, rs.getObject("reviewer_id"));
                        insert.setObject(3, rs.getObject("project_id"));
                        if (evaluationId != 0) {
                            insert.setInt(4, evaluationId);
                        } else {
                            insert.setNull(4, Types.INTEGER);
                        }
                        insert.setLong(5, questionId);
                        insert.setLong(6, rs.getLong("scorecard_id"));

                        insert.executeUpdate();
                    }

                    count++;

                }
                close(rs);
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'scorecard_response' table failed for question " + questionId + " .\n" +
                    sqle.getMessage());
        } finally {
            close(projects);
            close(insert);
            close(update);
            close(select);
        }
    }

}
