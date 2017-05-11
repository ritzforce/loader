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


public class TCLoadSubmission extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadSubmission.class);

    @Override
    public void performLoad() throws Exception {
        doLoadSubmission();
    }

    /**
     * This method loads submissions
     *
     * @throws Exception if any error occurs
     */
    private void doLoadSubmission() throws Exception {
        log.info("load submissions");
        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;
        PreparedStatement delete = null;
        ResultSet rs = null;
        int submissionId=0;

        try {
            long start = System.currentTimeMillis();

            // check if submission table is empty, if yes, do a complete load (assume that is the first time a load is done)
            select = prepareStatement("select count(*) from submission", TARGET_DB);
            rs = select.executeQuery();
            rs.next();

            boolean firstRun = rs.getInt(1) == 0;

            if (firstRun) log.info("Loading submission table for the first time.  A complete load will be performed.");

            final String SELECT =
                    "select submission_id " +
                            "   ,ri.value as submitter_id" +
                            ",  u.project_id" +
                            ",  u.parameter as submission_url" +
                            ",  s.submission_status_id " +
                            ",  1 as submission_type " +
                            "from submission s" +
                            "   ,upload u" +
                            "   ,project p " +
                            "   ,resource r" +
                            "   ,resource_info ri " +
                            "where s.upload_id = u.upload_id " +
                            "   and s.submission_type_id = 1 " +
                            "   and u.project_id = p.project_id " +
                            "   and p.project_status_id <> 3 " +
                            "   and p.project_category_id in " + LOAD_CATEGORIES +
                            "   and u.resource_id = r.resource_id " +
                            "   and r.resource_id = ri.resource_id " +
                            "   and ri.resource_info_type_id = 1 " +
                            "   and u.upload_type_id = 1 " +
                            ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                            (firstRun ? "" :
                                    "and (s.modify_date > ? " +
                                            "OR u.modify_date > ? " +
                                            "OR r.modify_date >= ? " +
                                            "OR ri.modify_date >= ? " +
                                            (needLoadMovedProject() ? " OR s.modify_user <> 'Converter' " +
                                                    " OR u.modify_user <> 'Converter' " +
                                                    " OR r.modify_user <> 'Converter' " +
                                                    " OR ri.modify_user <> 'Converter' " +
                                                    ")"
                                                    : ")"));

            final String UPDATE = "update submission set submission_url=? where submission_id=?";

            final String INSERT = "insert into submission (submitter_id, project_id, submission_url, submission_type, submission_id)" +
                    "values (?, ?, ?, ?, ?) ";

            final String DELETE = "delete from submission where submission_id=?";

            select = prepareStatement(SELECT, SOURCE_DB);

            if (!firstRun) {
                select.setTimestamp(1, fLastLogTime);
                select.setTimestamp(2, fLastLogTime);
                select.setTimestamp(3, fLastLogTime);
                select.setTimestamp(4, fLastLogTime);
            }

            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            delete = prepareStatement(DELETE, TARGET_DB);
            rs = select.executeQuery();

            int count = 0;
            while (rs.next()) {
                submissionId = rs.getInt("submission_id");

                // If the submission was deleted in the source DB then delete it from the target DB as well
                if (rs.getInt("submission_status_id") == 5) {
                    delete.clearParameters();
                    delete.setInt(1, submissionId);
                    delete.executeUpdate();
                    continue;
                }

                count++;

                // make sure submission dir exist
                String submissionUrl = rs.getString("submission_url");
                if (submissionUrl != null && submissionUrl.indexOf("/") <= 0) {
                    // submission_dir does not prefix
                    submissionUrl = this.submissionDir + submissionUrl;
                }

                update.clearParameters();
                update.setString(1, submissionUrl);
                update.setInt(2, submissionId);

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    //need to insert
                    insert.clearParameters();
                    insert.setInt(1, rs.getInt("submitter_id"));
                    insert.setInt(2, rs.getInt("project_id"));
                    insert.setString(3, submissionUrl);
                    insert.setInt(4, rs.getInt("submission_type"));
                    insert.setInt(5, rs.getInt("submission_id"));
                    insert.executeUpdate();
                }
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'submission' table failed submission id " + submissionId + " .\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(insert);
            close(update);
            close(delete);
            close(select);
        }
    }

}
