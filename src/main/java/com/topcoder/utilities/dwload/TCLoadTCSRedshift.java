/*
 * Copyright (C) 2004 - 2015 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload;

import com.topcoder.shared.util.dwload.TCLoad;
import com.topcoder.shared.util.logging.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Locale;


/**
 * <p><strong>Purpose</strong>:
 * A modified version of <strong>TCLoadTCS</strong> where the DW database is
 * on Redshift.
 * </p>
 * <p>
 * Derived from version 1.4.3 of <strong>TCLoadTCS</strong>, see the original
 * class documentation for a full change log. 
 * </p>
 * @author gbts
 * @version 1.4.3
 */
public abstract class TCLoadTCSRedshift extends TCLoad {

    private static Logger log = Logger.getLogger(TCLoadTCSRedshift.class);

    /**
     * <p>A <code>String</code> representing all those project categories than should be loaded to the
     * data warehouse.</p>
     */
    protected static final String LOAD_CATEGORIES = "(1, 2, 5, 6, 7, 13, 14, 23, 19, 24, 25, 26, 29, 35, 16, 17, 18, 20, 21, 30, 31, 32, 34, 22, 36, 9, 39, 38, 40)";

    /**
     * <p>We have too many projects to fit in a single IN statement in a retrieval query any more, so we'll split the
     * project result load into steps of this size.</p>
     */
    protected static final int PROJECT_RESULT_LOAD_STEP_SIZE = 500;

    protected static final int OVERALL_RATING_RANK_TYPE_ID = 1;
    protected static final int ACTIVE_RATING_RANK_TYPE_ID = 2;

    protected static int TCS_LOG_TYPE = 8;

    protected static final String PROJECT_SELECT =
            "select distinct project_id from project_result";

    /**
     * SQL fragment to be added to a where clause to not select projects with eligibility constraints
     *
     * @since 1.1.5
     */
    protected static final String ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT =
            " and p.project_id not in (select ce.contest_id from contest_eligibility ce " +
                    " where ce.is_studio = 0) ";

    protected static final long DELETED_PROJECT_STATUS = 3;

    protected String submissionDir = null;


    public TCLoadTCSRedshift() {
        DEBUG = false;
    }

    /**
     * Return if it will load moved project which not be covered by last old_dw load.
     *
     * @return true if load log time is before the 2006-11-4.
     */
    protected boolean needLoadMovedProject() {
        return this.fLastLogTime == null ? true : this.fLastLogTime.before(java.sql.Date.valueOf("2006-11-11"));
    }

    /**
     * This method is passed any parameters passed to this load
     */
    public boolean setParameters(Hashtable params) {
        String temp = (String) params.get("submission_dir");
        if (temp == null) {
            submissionDir = "file:/tcssubmissions/";
            log.info("Use default submissionDir: file:/tcssubmissions/");
        } else {
            log.info("set submissionDir: " + temp);
            if (!temp.endsWith("/")) {
                temp += "/";
            }
            submissionDir = temp;
        }

        if(params.get("tcs_log_type") != null) {
            TCS_LOG_TYPE = Integer.valueOf((String)params.get("tcs_log_type"));
        }

        return true;
    }


    /**
     * Helper method that deletes all project related objects in the dw.
     *
     * @param projectId the projectId to delete
     * @since 1.1.2
     */
    protected void deleteProject(long projectId) throws SQLException {
        simpleDelete("subjective_response", "project_id", projectId);
        simpleDelete("testcase_response", "project_id", projectId);
        simpleDelete("scorecard_response", "project_id", projectId);
        simpleDelete("submission_screening", "project_id", projectId);
        simpleDelete("submission_review", "project_id", projectId);
        simpleDelete("testcase_appeal", "project_id", projectId);
        simpleDelete("streak", "start_project_id", projectId);
        simpleDelete("streak", "end_project_id", projectId);
        simpleDelete("user_rating", "last_rated_project_id", projectId);
        simpleDelete("contest_project_xref", "project_id", projectId);
        simpleDelete("project_review", "project_id", projectId);
        simpleDelete("submission", "project_id", projectId);
        simpleDelete("appeal", "project_id", projectId);
        simpleDelete("project_result", "project_id", projectId);
        simpleDelete("design_project_result", "project_id", projectId);
        simpleDelete("project_spec_review_xref", "project_id", projectId);
        simpleDelete("project_platform", "project_id", projectId);
        simpleDelete("project_technology", "project_id", projectId);
        simpleDelete("project", "project_id", projectId);
    }

    /**
     * Simple helper method to delete rows from a table using an id column
     *
     * @param table  the target table
     * @param column the target column
     * @param id     the the id value to delete
     * @throws SQLException if delete execution fails
     * @since 1.1.2
     */
    protected void simpleDelete(String table, String column, long id) throws SQLException {
        PreparedStatement delete = prepareStatement("delete from " + table + " where " + column + " = ?", TARGET_DB);
        delete.setLong(1, id);
        long count = delete.executeUpdate();
        log.info("" + count + " records deleted in " + table + " table");
    }

    private static final DateFormat[] DATE_FORMATS = new DateFormat[]{
            new SimpleDateFormat("MM/dd/yyyy hh:mm", Locale.US),
            new SimpleDateFormat("MM.dd.yyyy hh:mm a", Locale.US),
            new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US),
            new SimpleDateFormat("MM.dd.yyyy HH:mm z", Locale.US),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    };

    protected static Timestamp convertToDate(String str) {
        if (str == null) {
            return null;
        }
        for (int i = 0; i < DATE_FORMATS.length; i++) {
            try {
                return new Timestamp(DATE_FORMATS[i].parse(str).getTime());
            } catch (Exception e) {
            }
        }
        return null;
    }

    protected int getEvaluationId(int questionType, String answer) {
        if (answer == null || answer.trim().length() == 0) {
            return 0;
        }
        switch (questionType) {
            case 1: // scale 1-4
                if (answer.equals("1") || answer.equals("1/4")) {
                    return 1;
                }
                if (answer.equals("2") || answer.equals("2/4")) {
                    return 2;
                }
                if (answer.equals("3") || answer.equals("3/4")) {
                    return 3;
                }
                if (answer.equals("4") || answer.equals("4/4")) {
                    return 4;
                }
            case 2: // scale 1-10
                if (answer.equals("1") || answer.equals("1/10")) {
                    return 11;
                }
                if (answer.equals("2") || answer.equals("2/10")) {
                    return 12;
                }
                if (answer.equals("3") || answer.equals("3/10")) {
                    return 13;
                }
                if (answer.equals("4") || answer.equals("4/10")) {
                    return 14;
                }
                if (answer.equals("5") || answer.equals("5/10")) {
                    return 15;
                }
                if (answer.equals("6") || answer.equals("6/10")) {
                    return 16;
                }
                if (answer.equals("7") || answer.equals("7/10")) {
                    return 17;
                }
                if (answer.equals("8") || answer.equals("8/10")) {
                    return 18;
                }
                if (answer.equals("9") || answer.equals("9/10")) {
                    return 19;
                }
                if (answer.equals("10") || answer.equals("10/10")) {
                    return 20;
                }
            case 3: // test case
                return 0;
            case 4: // Yes/No
                if ("Yes".equals(answer) || "1".equals(answer)) {
                    return 5;
                } else {
                    return 6;
                }
        }
        try {
            return Integer.parseInt(answer);
        } catch (Exception e) {
            return 0;
        }
    }

    protected void setCalendar(PreparedStatement ps, int parameterNumber, Timestamp value) throws SQLException {
        if (value != null) {
            ps.setInt(parameterNumber, lookupCalendarId(value, TARGET_DB));
        } else {
            ps.setNull(parameterNumber, Types.INTEGER);
        }
    }
}
