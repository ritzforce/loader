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


public class TCLoadScorecardTemplate extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadScorecardTemplate.class);

    @Override
    public void performLoad() throws Exception {
        doLoadScorecardTemplate();
    }

    private void doLoadScorecardTemplate() throws Exception {
        log.info("load scorecard template");
        ResultSet rs = null;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;


        final String SELECT = "select scorecard_id as scorecard_template_id, " +
                "s.scorecard_type_id,   " +
                "stl.name as scorecard_type_desc " +
                "from scorecard s, " +
                "   scorecard_type_lu stl " +
                "where s.scorecard_type_id = stl.scorecard_type_id " +
                "   and (s.modify_date > ?  " +
                "   or stl.modify_date > ? " +
                (needLoadMovedProject() ? " OR s.modify_user <> 'Converter' " +
                        " OR stl.modify_user <> 'Converter' " +
                        ")"
                        : ")"); // TODO make sure all scorecard that moved are loaded which modify date is too earlier

        final String UPDATE =
                "update scorecard_template set scorecard_type_id=?, scorecard_type_desc=? where scorecard_template_id = ?";

        final String INSERT =
                "insert into scorecard_template (scorecard_type_id, scorecard_type_desc, scorecard_template_id) values (?, ?, ?)";


        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            select.setTimestamp(1, fLastLogTime);
            select.setTimestamp(2, fLastLogTime);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            int count = 0;

            rs = select.executeQuery();

            while (rs.next()) {

                update.clearParameters();

                update.setObject(1, rs.getObject("scorecard_type_id"));
                update.setObject(2, rs.getObject("scorecard_type_desc"));
                update.setLong(3, rs.getLong("scorecard_template_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();

                    insert.setObject(1, rs.getObject("scorecard_type_id"));
                    insert.setObject(2, rs.getObject("scorecard_type_desc"));
                    insert.setLong(3, rs.getLong("scorecard_template_id"));

                    insert.executeUpdate();
                }
                count++;

            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'scorecard_template' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(update);
            close(select);
        }
    }

}
