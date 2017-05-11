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


public class TCLoadContestProject extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadContestProject.class);

    @Override
    public void performLoad() throws Exception {
        doLoadContestProject();
    }

    /**
     * Loads contest projects
     *
     * @throws Exception if any error occurs
     */
    public void doLoadContestProject() throws Exception {
        log.info("load contest project");
        //load contest_project_xref
        long start = System.currentTimeMillis();

        final String SELECT = "select x.contest_id, x.project_id  " +
                "from contest_project_xref x, project p " +
                "where x.project_id = ? and p.project_id = x.project_id " +
                ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                " and (p.modify_date > ? or x.create_date > ?)";

        final String INSERT = "insert into contest_project_xref (contest_id, project_id) " +
                "values (?, ?)";
        final String DELETE = "delete from contest_project_xref where contest_id = ? and project_id = ?";


        PreparedStatement delete = null;
        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement projectSelect = null;
        ResultSet rs = null;
        ResultSet projects = null;

        long projectId = 0;
        try {
            select = prepareStatement(SELECT, SOURCE_DB);
            projectSelect = prepareStatement(PROJECT_SELECT, SOURCE_DB);

            insert = prepareStatement(INSERT, TARGET_DB);
            delete = prepareStatement(DELETE, TARGET_DB);

            projects = projectSelect.executeQuery();
            int count = 0;
            while (projects.next()) {
                projectId = projects.getLong("project_id");
                select.clearParameters();
                select.setLong(1, projectId);
                select.setTimestamp(2, fLastLogTime);
                select.setTimestamp(3, fLastLogTime);

                rs = select.executeQuery();
                while (rs.next()) {
                    insert.clearParameters();
                    insert.setLong(1, rs.getLong("contest_id"));
                    insert.setLong(2, projectId);
                    try {
                        insert.executeUpdate();
                    } catch (SQLException e) {
                        delete.setLong(1, rs.getLong("contest_id"));
                        delete.setLong(2, projectId);
                        delete.executeUpdate();
                        insert.executeUpdate();
                    }
                    count++;

                }

            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException e) {
            DBMS.printSqlException(true, e);
            throw new Exception("Load of 'contest_project_xref' failed on project " + projectId);
        } finally {
            close(rs);
            close(projects);
            close(delete);
            close(select);
            close(insert);
            close(projectSelect);
        }
    }

}
