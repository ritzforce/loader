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


public class TCLoadSubjectiveResponse extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadSubjectiveResponse.class);

    @Override
    public void performLoad() throws Exception {
        doLoadSubjectiveResponse();
    }

    /**
     * Loads subjective responses
     *
     * @throws Exception if any error occurs
     */
    private void doLoadSubjectiveResponse() throws Exception {
        log.info("load subjective_response");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement delete = null;
        PreparedStatement insert = null;
        ResultSet projects;
        PreparedStatement projectSelect;


        final String SELECT =
                "select ri.scorecard_question_id " +
                        "    ,r.review_id as scorecard_id " +
                        "    ,ri1.value as user_id " +
                        "    ,ri2.value as reviewer_id " +
                        "    ,u.project_id " +
                        "    ,ric.content as response_text " +
                        "    ,case when ric.comment_type_id = 1 then 3 when ric.comment_type_id = 3 then 1 else 2 end as response_type_id " +
                        "    ,case when ric.comment_type_id = 1 then 'Comment' when ric.comment_type_id = 3 then 'Required' else 'Recommended' end as response_type_desc " +
                        "    ,ric.review_item_comment_id subjective_resp_id " +
                        "    from review_item_comment ric, " +
                        "       comment_type_lu ctl," +
                        "       review_item  ri, " +
                        "       review r," +
                        "       submission s," +
                        "       upload u," +
                        "       project p, " +
                        "       resource_info ri1," +
                        "       resource_info ri2," +
                        "       resource res " +
                        "    where  ric.comment_type_id = ctl.comment_type_id " +
                        "   and s.submission_type_id = 1 " +
                        "   and ric.review_item_id = ri.review_item_id " +
                        "   and ri.review_id = r.review_id " +
                        "   and r.submission_id = s.submission_id " +
                        "   and u.upload_id = s.upload_id " +
                        "   and u.project_id = p.project_id " +
                        "   and p.project_status_id <> 3 " +
                        "   and p.project_category_id in " + LOAD_CATEGORIES +
                        "   and r.resource_id = res.resource_id " +
                        "   and res.resource_role_id in (2, 3, 4, 5, 6, 7) " +
                        "   and ric.comment_type_id in (1, 2, 3) " +
                        "   and ri1.resource_id = u.resource_id " +
                        "   and ri1.resource_info_type_id = 1 " +
                        "   and ri2.resource_id = r.resource_id " +
                        "   and ri2.resource_info_type_id = 1 " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        "   and u.project_id = ? " +
                        "   and (ric.modify_date > ? " +
                        "   OR ri.modify_date > ? " +
                        "   OR r.modify_date > ? " +
                        "   OR s.modify_date > ? " +
                        "   OR u.modify_date > ? " +
                        "   OR ri1.modify_date > ? " +
                        "   OR ri2.modify_date > ? " +
                        "   OR res.modify_date > ? " +
                        (needLoadMovedProject() ? " OR ric.modify_user <> 'Converter' " +
                                " OR ri.modify_user <> 'Converter' " +
                                " OR r.modify_user <> 'Converter' " +
                                " OR s.modify_user <> 'Converter' " +
                                " OR u.modify_user <> 'Converter' " +
                                " OR ri1.modify_user <> 'Converter' " +
                                " OR ri2.modify_user <> 'Converter' " +
                                " OR res.modify_user <> 'Converter' " +
                                ")"
                                : ")") +
                        "order by scorecard_question_id, scorecard_id, subjective_resp_id  ";
        final String DELETE =
                "delete from subjective_response where scorecard_question_id = ? and scorecard_id = ?";

        final String INSERT =
                "insert into subjective_response (user_id, reviewer_id, project_id, response_text, response_type_id, response_type_desc, sort, scorecard_question_id, scorecard_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";


        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            delete = prepareStatement(DELETE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            projectSelect = prepareStatement(PROJECT_SELECT, SOURCE_DB);

            int count = 0;

            long prevScorecardQuestion = -1;
            long prevScorecard = -1;
            int sort = 0;


            projects = projectSelect.executeQuery();

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
                select.setTimestamp(9, fLastLogTime);

                rs = select.executeQuery();

                while (rs.next()) {

                    if ((rs.getLong("scorecard_question_id") != prevScorecardQuestion) ||
                            (rs.getLong("scorecard_id") != prevScorecard)) {
                        sort = 0;
                        prevScorecardQuestion = rs.getLong("scorecard_question_id");
                        prevScorecard = rs.getLong("scorecard_id");
                        delete.clearParameters();

                        delete.setLong(1, rs.getLong("scorecard_question_id"));
                        delete.setLong(2, rs.getLong("scorecard_id"));

                        delete.executeUpdate();

                    } else {
                        sort++;
                    }

                    insert.clearParameters();

                    insert.setObject(1, rs.getObject("user_id"));
                    insert.setObject(2, rs.getObject("reviewer_id"));
                    insert.setObject(3, rs.getObject("project_id"));
                    insert.setObject(4, rs.getObject("response_text"));
                    insert.setObject(5, rs.getObject("response_type_id"));
                    insert.setObject(6, rs.getObject("response_type_desc"));
                    insert.setInt(7, sort);
                    insert.setLong(8, rs.getLong("scorecard_question_id"));
                    insert.setLong(9, rs.getLong("scorecard_id"));

                    insert.executeUpdate();
                    count++;
                }
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'subjective_response' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(delete);
            close(select);
        }
    }

}
