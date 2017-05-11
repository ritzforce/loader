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


public class TCLoadSpecReviews extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadSpecReviews.class);

    @Override
    public void performLoad() throws Exception {
        doLoadSpecReviews();
    }

    /**
     * <p/>
     * Load links between projects and spec reviews to the DW.
     * IMPORTANT: This will deprecate with the new spec review process when spec reviews are just phases in the parent
     * project.
     * </p>
     *
     * @throws Exception if any error occurs
     */
    private void doLoadSpecReviews() throws Exception {
        log.info("load spec reviews");
        PreparedStatement select = null;
        PreparedStatement selectDW = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        ResultSet rs = null;

        try {
            long start = System.currentTimeMillis();

            //get data from source DB
            final String SELECT = "select base_p.project_id as base_project_id, spec_review_p.project_id as spec_review_project_id " +
                    "from project base_p, project spec_review_p, linked_project_xref lpx where " +
                    "spec_review_p.project_category_id = 27 " +
                    "and spec_review_p.project_id = lpx.dest_project_id " +
                    "and lpx.link_type_id = 3 " +
                    "and lpx.source_project_id = base_p.project_id ";

            final String SELECT_DW = "select 1 from project where project_id = ? ";

            final String UPDATE = "update project_spec_review_xref set spec_review_project_id = ? where project_id = ? ";

            final String INSERT = "insert into project_spec_review_xref (project_id, spec_review_project_id) values (?, ?) ";

            select = prepareStatement(SELECT, SOURCE_DB);
            selectDW = prepareStatement(SELECT_DW, TARGET_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            rs = select.executeQuery();
            int count = 0;
            while (rs.next()) {
                selectDW.setLong(1, rs.getLong("base_project_id"));
                if (selectDW.executeQuery().next()==false) {
                    continue;
                }

                update.setLong(1, rs.getLong("spec_review_project_id"));
                update.setLong(2, rs.getLong("base_project_id"));
                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    //need to insert
                    insert.setLong(1, rs.getLong("base_project_id"));
                    insert.setLong(2, rs.getLong("spec_review_project_id"));
                    insert.executeUpdate();
                }
                count++;
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            sqle.printStackTrace();
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'project_spec_review_xref' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(selectDW);
            close(insert);
            close(update);
        }
    }

}
