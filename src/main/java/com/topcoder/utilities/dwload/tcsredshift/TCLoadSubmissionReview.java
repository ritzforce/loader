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
import java.util.HashMap;
import java.util.Map;


public class TCLoadSubmissionReview extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadSubmissionReview.class);

    @Override
    public void performLoad() throws Exception {
        doLoadSubmissionReview();
    }

    /**
     * Loads submission reviews
     *
     * @throws Exception if any error occurs
     */
    private void doLoadSubmissionReview() throws Exception {
        log.info("load submission review");
        ResultSet submissionInfo = null;
        PreparedStatement submissionSelect = null;
        PreparedStatement submissionUpdate = null;
        PreparedStatement submissionInsert = null;
        PreparedStatement reviewRespSelect = null;
        PreparedStatement maxReviewRespSelect = null;
        PreparedStatement reviewRespUpdate = null;

        final String SUBMISSION_SELECT =
                "select u.project_id " +
                        "  ,ri1.value as user_id " +
                        "  ,ri2.value as reviewer_id " +
                        "   ,r.initial_score as raw_score " +
                        "   ,r.score as final_score " +
                        "   ,(select count(*) from review_item_comment ric,review_item ri " +
                        "       where ric.review_item_id = ri.review_item_id and ri.review_id = r.review_id and ric.comment_type_id = 4) as num_appeals " +
                        "   ,(select count(*) from review_item_comment ric,review_item ri  " +
                        "       where ric.review_item_id = ri.review_item_id and ri.review_id = r.review_id and ric.comment_type_id = 4 and ric.extra_info = 'Succeeded')  " +
                        "       as num_successful_appeals " +
                        "   ,(select count(*) from review_item_comment ric,review_item ri " +
                        "       where ric.review_item_id = ri.review_item_id " +
                        "       and ri.review_id = r.review_id and ric.comment_type_id = 4 and ric.extra_info is not null)  " +
                        "       as non_null_successful_appeals " +
                        "   ,case  " +
                        "       when exists (select 1 from resource where resource_id = r.resource_id and resource_role_id = 7) then 1 " +
                        "       when exists (select 1 from resource where resource_id = r.resource_id and resource_role_id = 6) then 2 " +
                        "       when exists (select 1 from resource where resource_id = r.resource_id and resource_role_id = 5) then 3 " +
                        "       else 4 end as review_resp_id " +
                        "   ,r.review_id as scorecard_id " +
                        "   ,r.scorecard_id as scorecard_template_id " +
                        " from review r " +
                        "   ,submission s " +
                        "   ,upload u " +
                        "  ,project p " +
                        "  ,resource_info ri1" +
                        "  ,resource_info ri2" +
                        "   ,resource res " +
                        "where r.submission_id = s.submission_id " +
                        "   and s.submission_type_id = 1 " +
                        "   and u.upload_id = s.upload_id " +
                        "   and u.project_id = p.project_id " +
                        "   and p.project_status_id <> 3 " +
                        "   and p.project_category_id in " + LOAD_CATEGORIES +
                        "   and res.resource_id = r.resource_id " +
                        "   and resource_role_id in (4, 5, 6, 7) " +
                        "   and ri1.resource_id = u.resource_id " +
                        "   and ri1.resource_info_type_id = 1 " +
                        "   and ri2.resource_id = r.resource_id " +
                        "   and ri2.resource_info_type_id = 1 " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        "   and (r.modify_date > ? " +
                        "   or s.modify_date > ? " +
                        "   or u.modify_date > ?" +
                        "   OR ri1.modify_date > ? " +
                        "   OR ri2.modify_date > ? " +
                        "   or res.modify_date > ? " +
                        "  or (select max(ric.modify_date) " +
                        "       from review_item_comment ric, review_item ri" +
                        "       where ric.review_item_id = ri.review_item_id and ri.review_id = r.review_id and ric.comment_type_id = 4) > ? " +
                        (needLoadMovedProject() ? " OR r.modify_user <> 'Converter' " +
                                " OR s.modify_user <> 'Converter' " +
                                " OR u.modify_user <> 'Converter' " +
                                " OR ri1.modify_user <> 'Converter' " +
                                " OR ri2.modify_user <> 'Converter' " +
                                " OR res.modify_user <> 'Converter' " +
                                ")"
                                : ")") +
                        " order by u.project_id";

        final String SUBMISSION_UPDATE =
                "update submission_review set raw_score = ?, final_score = ?, num_appeals = ?, num_successful_appeals = ?, review_resp_id = ?,  scorecard_id = ?, scorecard_template_id = ? " +
                        "where project_id = ? and user_id = ? and reviewer_id = ?";

        final String SUBMISSION_INSERT =
                "insert into submission_review (project_id, user_id, reviewer_id, raw_score, final_score, num_appeals, " +
                        "num_successful_appeals, review_resp_id, scorecard_id, scorecard_template_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        final String REVIEW_RESP_UPDATE =
                "update submission_review set review_resp_id = ? where project_id = ? and reviewer_id = ?";

        try {
            long start = System.currentTimeMillis();

            submissionSelect = prepareStatement(SUBMISSION_SELECT, SOURCE_DB);
            submissionUpdate = prepareStatement(SUBMISSION_UPDATE, TARGET_DB);
            submissionInsert = prepareStatement(SUBMISSION_INSERT, TARGET_DB);
            reviewRespUpdate = prepareStatement(REVIEW_RESP_UPDATE, TARGET_DB);

            int count = 0;
            submissionSelect.clearParameters();
            submissionSelect.setTimestamp(1, fLastLogTime);
            submissionSelect.setTimestamp(2, fLastLogTime);
            submissionSelect.setTimestamp(3, fLastLogTime);
            submissionSelect.setTimestamp(4, fLastLogTime);
            submissionSelect.setTimestamp(5, fLastLogTime);
            submissionSelect.setTimestamp(6, fLastLogTime);
            submissionSelect.setTimestamp(7, fLastLogTime);
            //log.debug("before submission select");
            submissionInfo = submissionSelect.executeQuery();
            //log.debug("after submission select");

            int nextFreeReviewRespId = 4;
            Map<Long, Integer> reviewerResps = new HashMap<Long, Integer>();

            boolean nextResultExists = submissionInfo.next();
            while (nextResultExists) {
                long projectId = submissionInfo.getLong("project_id");
                count++;

                submissionUpdate.clearParameters();

                int reviewRespId = submissionInfo.getInt("review_resp_id");

                log.info( "Project_id = " + projectId + "raw_score= " + submissionInfo.getObject("raw_score"));
                submissionUpdate.setObject(1, submissionInfo.getObject("raw_score"), Types.DECIMAL, 2);
                submissionUpdate.setObject(2, submissionInfo.getObject("final_score"), Types.DECIMAL, 2);
                submissionUpdate.setObject(3, submissionInfo.getObject("num_appeals"), Types.INTEGER);
                if (submissionInfo.getInt("non_null_successful_appeals") == 0) {
                    submissionUpdate.setNull(4, Types.DECIMAL);
                } else {
                    submissionUpdate.setInt(4, submissionInfo.getInt("num_successful_appeals"));
                }
                submissionUpdate.setInt(5, reviewRespId);
                submissionUpdate.setObject(6, submissionInfo.getObject("scorecard_id"), Types.BIGINT);
                submissionUpdate.setObject(7, submissionInfo.getObject("scorecard_template_id"), Types.BIGINT);
                submissionUpdate.setLong(8, submissionInfo.getLong("project_id"));
                submissionUpdate.setLong(9, submissionInfo.getLong("user_id"));
                submissionUpdate.setLong(10, submissionInfo.getLong("reviewer_id"));

                //log.debug("before submission update");
                int retVal = submissionUpdate.executeUpdate();
                //log.debug("after submission update");

                if (retVal == 0) {
                    submissionInsert.clearParameters();

                    submissionInsert.setLong(1, submissionInfo.getLong("project_id"));
                    submissionInsert.setLong(2, submissionInfo.getLong("user_id"));
                    submissionInsert.setLong(3, submissionInfo.getLong("reviewer_id"));
                    submissionInsert.setObject(4, submissionInfo.getObject("raw_score"), Types.DECIMAL, 2);
                    submissionInsert.setObject(5, submissionInfo.getObject("final_score"), Types.DECIMAL, 2);
                    submissionInsert.setObject(6, submissionInfo.getObject("num_appeals"), Types.INTEGER);
                    if (submissionInfo.getInt("non_null_successful_appeals") == 0) {
                        submissionInsert.setNull(7, Types.DECIMAL);
                    } else {
                        submissionInsert.setObject(7, submissionInfo.getObject("num_successful_appeals"));
                    }

                    submissionInsert.setInt(8, reviewRespId);
                    submissionInsert.setObject(9, submissionInfo.getObject("scorecard_id"), Types.BIGINT);
                    submissionInsert.setObject(10, submissionInfo.getObject("scorecard_template_id"), Types.BIGINT);

                    //log.debug("before submission insert");
                    submissionInsert.executeUpdate();
                    //log.debug("after submission insert");
                }

                long reviewerId = submissionInfo.getLong("reviewer_id");
                if (reviewerResps.containsKey(reviewerId)) {
                    if (reviewRespId == 4) {
                        reviewRespId = nextFreeReviewRespId;
                        nextFreeReviewRespId++;
                    }
                    reviewerResps.put(reviewerId, reviewRespId);
                }


                nextResultExists = submissionInfo.next();

                if (!nextResultExists || submissionInfo.getLong("project_id") != projectId) {
                    for (Long reviewerId2 : reviewerResps.keySet()) {
                        reviewRespUpdate.clearParameters();
                        reviewRespUpdate.setLong(1, reviewerResps.get(reviewerId2));
                        reviewRespUpdate.setLong(2, projectId);
                        reviewRespUpdate.setLong(3, reviewerId2);
                        reviewRespUpdate.executeUpdate();
                    }

                    nextFreeReviewRespId = 4;
                    reviewerResps.clear();
                }

            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'submission review' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(submissionInfo);
            close(submissionSelect);
            close(submissionUpdate);
            close(submissionInsert);
            close(reviewRespSelect);
            close(maxReviewRespSelect);
            close(reviewRespUpdate);
        }
    }

}
