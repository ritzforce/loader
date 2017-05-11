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


public class TCLoadSubmissionScreening extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadSubmissionScreening.class);

    @Override
    public void performLoad() throws Exception {
        doLoadSubmissionScreening();
    }

    /**
     * Loads submission screening
     *
     * @throws Exception if any error occurs
     */
    private void doLoadSubmissionScreening() throws Exception {
        log.info("load submission screening");

        ResultSet screenings = null;
        PreparedStatement screeningUpdate = null;
        PreparedStatement screeningInsert = null;
        PreparedStatement screeningSelect = null;


        final String SCREENING_SELECT =
                "select u.project_id " +
                        "   ,ri1.value as user_id " +
                        "   ,ri2.value as reviewer_id " +
                        "   ,r.score as final_score " +
                        "   ,r.review_id as scorecard_id " +
                        "   ,r.scorecard_id as scorecard_template_id " +
                        "from review r," +
                        "   submission s," +
                        "   upload u," +
                        "   project p, " +
                        "   resource_info ri1," +
                        "   resource_info ri2," +
                        "   resource res " +
                        "where r.submission_id = s.submission_id " +
                        "   and s.submission_type_id = 1 " +
                        "   and u.upload_id = s.upload_id " +
                        "   and u.project_id = p.project_id " +
                        "   and p.project_status_id <> 3 " +
                        "   and p.project_category_id in " + LOAD_CATEGORIES +
                        "   and res.resource_id = r.resource_id " +
                        "   and resource_role_id in (2, 3) " +
                        "   and ri1.resource_id = u.resource_id " +
                        "   and ri1.resource_info_type_id = 1 " +
                        "   and ri2.resource_id = r.resource_id " +
                        "   and ri2.resource_info_type_id = 1 " +
                        ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                        "   and (r.modify_date > ? " +
                        "   OR s.modify_date > ? " +
                        "   or u.modify_date > ? " +
                        "   OR ri1.modify_date > ? " +
                        "   OR ri2.modify_date > ? " +
                        "   or res.modify_date > ? " +
                        (needLoadMovedProject() ? " OR r.modify_user <> 'Converter' " +
                                " OR s.modify_user <> 'Converter' " +
                                " OR u.modify_user <> 'Converter' " +
                                " OR ri1.modify_user <> 'Converter' " +
                                " OR ri2.modify_user <> 'Converter' " +
                                " OR res.modify_user <> 'Converter' " +
                                ")"
                                : ")");

        final String SCREENING_UPDATE =
                "update submission_screening set reviewer_id = ?, final_score = ?, scorecard_id = ?, scorecard_template_id = ? " +
                        "where project_id = ? and user_id = ?";

        final String SCREENING_INSERT =
                "insert into submission_screening (project_id, user_id, reviewer_id, final_score, scorecard_id, scorecard_template_id) " +
                        "values (?, ?, ?, ?, ?, ?)";

        try {
            long start = System.currentTimeMillis();

            screeningSelect = prepareStatement(SCREENING_SELECT, SOURCE_DB);
            screeningSelect.setTimestamp(1, fLastLogTime);
            screeningSelect.setTimestamp(2, fLastLogTime);
            screeningSelect.setTimestamp(3, fLastLogTime);
            screeningSelect.setTimestamp(4, fLastLogTime);
            screeningSelect.setTimestamp(5, fLastLogTime);
            screeningSelect.setTimestamp(6, fLastLogTime);
            screeningUpdate = prepareStatement(SCREENING_UPDATE, TARGET_DB);
            screeningInsert = prepareStatement(SCREENING_INSERT, TARGET_DB);

            int count = 0;

            screenings = screeningSelect.executeQuery();

            while (screenings.next()) {
                long project_id = screenings.getLong("project_id");
                count++;
                screeningUpdate.clearParameters();


                screeningUpdate.setObject(1, screenings.getObject("reviewer_id"), Types.INTEGER);
                screeningUpdate.setObject(2, screenings.getObject("final_score"), Types.DECIMAL);
                screeningUpdate.setObject(3, screenings.getObject("scorecard_id"), Types.INTEGER);
                screeningUpdate.setObject(4, screenings.getObject("scorecard_template_id"), Types.INTEGER);
                screeningUpdate.setLong(5, project_id);
                screeningUpdate.setLong(6, screenings.getLong("user_id"));

                int retVal = screeningUpdate.executeUpdate();

                if (retVal == 0) {

                    screeningInsert.clearParameters();

                    screeningInsert.setLong(1, project_id);
                    screeningInsert.setLong(2, screenings.getLong("user_id"));
                    screeningInsert.setObject(3, screenings.getObject("reviewer_id"), Types.INTEGER);
                    screeningInsert.setObject(4, screenings.getObject("final_score"), Types.DECIMAL);
                    screeningInsert.setObject(5, screenings.getObject("scorecard_id"), Types.INTEGER);
                    screeningInsert.setObject(6, screenings.getObject("scorecard_template_id"), Types.INTEGER);

                    screeningInsert.executeUpdate();
                }
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of submission_screening table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(screenings);
            close(screeningUpdate);
            close(screeningInsert);
            close(screeningSelect);
        }
    }

}
