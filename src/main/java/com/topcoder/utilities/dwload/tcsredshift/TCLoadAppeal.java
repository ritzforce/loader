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


public class TCLoadAppeal extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadAppeal.class);

    @Override
    public void performLoad() throws Exception {
        doLoadAppeal();
    }

    /**
     * Loads appeals
     *
     * @throws Exception if any error occurs
     */
    public void doLoadAppeal() throws Exception {
        log.info("load Appeal");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        ResultSet projects;
        PreparedStatement projectSelect;

        final String SELECT =
                "select ric.review_item_comment_id as appeal_id " +
                        "    ,ri.scorecard_question_id  as scorecard_question_id " +
                        "    ,r.review_id as scorecard_id " +
                        "    ,res1.value as user_id " +
                        "    ,res2.value as reviewer_id " +
                        "    ,u.project_id " +
                        "    ,ri.answer as final_evaluation_id " +
                        "    ,ric.content as appeal_text " +
                        "    ,ric_resp.content as appeal_response " +
                        "    ,ric.extra_info as successful_ind " +
                        "    ,ric_resp.extra_info as raw_evaluation_id" +
                        "    ,sq.scorecard_question_type_id " +
                        "    from review_item_comment ric, " +
                        "       review_item  ri, " +
                        "       review r, " +
                        "       submission s,  " +
                        "       upload u, " +
                        "       project p, " +
                        "       resource res,  " +
                        "       resource_info res1,  " +
                        "       resource_info res2,  " +
                        "       outer review_item_comment ric_resp," +
                        "       scorecard_question sq" +
                        "    where ric.review_item_id = ri.review_item_id and " +
                        "   s.submission_type_id = 1 and " +
                        "   ri.review_id = r.review_id and " +
                        "   ri.scorecard_question_id = sq.scorecard_question_id and " +
                        "   r.submission_id = s.submission_id and " +
                        "   u.upload_id = s.upload_id and " +
                        "   u.project_id = p.project_id and " +
                        "   p.project_status_id <> 3 and " +
                        "   p.project_category_id in " + LOAD_CATEGORIES + " and " +
                        "   r.resource_id = res.resource_id and " +
                        "   res.resource_role_id in (4, 5, 6, 7) and " +
                        "   res1.resource_id = u.resource_id and " +
                        "   res1.resource_info_type_id = 1 and " +
                        "   res2.resource_id = r.resource_id and " +
                        "   res2.resource_info_type_id = 1 and " +
                        "   ric_resp.review_item_id = ri.review_item_id and " +
                        "   ric_resp.comment_type_id = 5 and " +
                        "   ric.comment_type_id = 4 " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT + " and " +
                        "   (ric.modify_date > ? OR " +
                        "   ri.modify_date > ? OR " +
                        "   r.modify_date > ? OR " +
                        "   s.modify_date > ? OR " +
                        "   u.modify_date > ? OR " +
                        "   res.modify_date > ? OR " +
                        "   res1.modify_date > ? OR " +
                        "   res2.modify_date > ? " +
                        //      "   OR ric_resp.modify_date > ?" +
                        (needLoadMovedProject() ? " OR ric.modify_user <> 'Converter' " +
                                " OR ri.modify_user <> 'Converter' " +
                                " OR r.modify_user <> 'Converter' " +
                                " OR s.modify_user <> 'Converter' " +
                                " OR u.modify_user <> 'Converter' " +
                                " OR res.modify_user <> 'Converter' " +
                                " OR res1.modify_user <> 'Converter' " +
                                " OR res2.modify_user <> 'Converter' " +
                                ")"
                                : ")");

        final String UPDATE =
                "update appeal set scorecard_question_id = ?, scorecard_id = ?, user_id=?, reviewer_id=?, project_id=?, " +
                        "raw_evaluation_id=?, final_evaluation_id=?, appeal_text=?, appeal_response=?, successful_ind = ? where appeal_id=?";

        final String INSERT =
                "insert into appeal (scorecard_question_id, scorecard_id, user_id, reviewer_id, project_id, " +
                        "raw_evaluation_id, final_evaluation_id, appeal_text, appeal_response, appeal_id, successful_ind) " +
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            int count = 0;
            select.clearParameters();
            select.setTimestamp(1, fLastLogTime);
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
                update.setLong(1, rs.getLong("scorecard_question_id"));
                update.setLong(2, rs.getLong("scorecard_id"));
                update.setObject(3, rs.getObject("user_id"), Types.INTEGER);
                update.setObject(4, rs.getObject("reviewer_id"), Types.INTEGER);
                update.setObject(5, rs.getObject("project_id"), Types.INTEGER);

                String answer = rs.getString("raw_evaluation_id");
                int evaluationId = getEvaluationId(rs.getInt("scorecard_question_type_id"), answer);
                if (evaluationId != 0) {
                    update.setInt(6, evaluationId);
                } else {
                    update.setNull(6, Types.INTEGER);
                }

                answer = rs.getString("final_evaluation_id");
                int finalEvaluationId = getEvaluationId(rs.getInt("scorecard_question_type_id"), answer);
                if (finalEvaluationId != 0) {
                    update.setInt(7, finalEvaluationId);
                } else {
                    update.setNull(7, Types.INTEGER);
                }
                String appealText = rs.getString("appeal_text");
                if (appealText == null) {
                    //update.setNull(8, Types.BLOB);
                    update.setString(8, ""); //Redshift jdbc driver can't handle null BLOB
                } else {
                    update.setBytes(8, DBMS.serializeTextString(appealText));
                }

                String appeal_response = rs.getString("appeal_response");
                if (appeal_response == null) {
                    //update.setNull(9, Types.BLOB);
                    update.setString(9, ""); //Redshift jdbc driver can't handle null BLOB
                } else {
                    update.setBytes(9, DBMS.serializeTextString(appeal_response));
                }

                String successfulInd = rs.getString("successful_ind");
                if (successfulInd == null) {
                    update.setNull(10, Types.INTEGER);
                } else {
                    if ("Succeeded".equals(successfulInd)) {
                        update.setInt(10, 1);
                    } else {
                        update.setInt(10, 0);
                    }
                }
                update.setLong(11, rs.getLong("appeal_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();

                    insert.setLong(1, rs.getLong("scorecard_question_id"));
                    insert.setLong(2, rs.getLong("scorecard_id"));
                    insert.setObject(3, rs.getObject("user_id"), Types.INTEGER);
                    insert.setObject(4, rs.getObject("reviewer_id"), Types.INTEGER);
                    insert.setObject(5, rs.getObject("project_id"), Types.INTEGER);
                    if (evaluationId != 0) {
                        insert.setInt(6, evaluationId);
                    } else {
                        insert.setNull(6, Types.INTEGER);
                    }
                    if (finalEvaluationId != 0) {
                        insert.setInt(7, finalEvaluationId);
                    } else {
                        insert.setNull(7, Types.INTEGER);
                    }

                    if (appealText == null) {
                        //insert.setNull(8, Types.BLOB);
                        insert.setString(8, "");
                    } else {
                        insert.setBytes(8, DBMS.serializeTextString(appealText));
                    }

                    if (appeal_response == null) {
                        //insert.setNull(9, Types.BLOB);
                        insert.setString(9, "");
                    } else {
                        insert.setBytes(9, DBMS.serializeTextString(appeal_response));
                    }

                    insert.setLong(10, rs.getLong("appeal_id"));
                    if (successfulInd == null) {
                        insert.setNull(11, Types.INTEGER);
                    } else {
                        if ("Succeeded".equals(successfulInd)) {
                            insert.setInt(11, 1);
                        } else {
                            insert.setInt(11, 0);
                        }
                    }

                    insert.executeUpdate();
                }
                count++;
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'appeal' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(update);
            close(select);
        }
    }

}
