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


public class TCLoadContestPrize extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadContestPrize.class);

    @Override
    public void performLoad() throws Exception {
        doLoadContestPrize();
    }

    private void doLoadContestPrize() throws Exception {
        log.info("load contest prize");
        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;
        ResultSet rs = null;

        try {
            long start = System.currentTimeMillis();
            final String SELECT =
                    "select cp.contest_prize_id, cp.contest_id, cp.prize_type_id, " +
                            "ptl.prize_type_desc, cp.place, cp.prize_amount, cp.prize_desc " +
                            "from contest_prize cp, prize_type_lu ptl " +
                            "where cp.prize_type_id = ptl.prize_type_id " +
                            "and (cp.modify_date > ?) ";

            final String INSERT = "insert into contest_prize (contest_prize_id, contest_id, prize_type_id, " +
                    "prize_type_desc, place, prize_amount, prize_desc) " +
                    "values (?, ?, ?, ?, ?, ?, ?) ";

            final String UPDATE = "update contest_prize set contest_id = ?, prize_type_id = ?,  prize_type_desc = ?, " +
                    "place = ?, prize_amount = ?, prize_desc = ? " +
                    " where contest_prize_id = ? ";

            //load prizes
            select = prepareStatement(SELECT, SOURCE_DB);
            select.setTimestamp(1, fLastLogTime);
            insert = prepareStatement(INSERT, TARGET_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            rs = select.executeQuery();

            int count = 0;
            while (rs.next()) {
                count++;
                //update record, if 0 rows affected, insert record
                update.clearParameters();
                update.setObject(1, rs.getObject("contest_id"));
                update.setObject(2, rs.getObject("prize_type_id"));
                update.setObject(3, rs.getObject("prize_type_desc"));
                update.setObject(4, rs.getObject("place"));
                update.setObject(5, rs.getObject("prize_amount"));
                update.setObject(6, rs.getObject("prize_desc"));
                update.setLong(7, rs.getLong("contest_prize_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();
                    insert.setLong(1, rs.getLong("contest_prize_id"));
                    insert.setLong(2, rs.getLong("contest_id"));
                    insert.setObject(3, rs.getObject("prize_type_id"));
                    insert.setObject(4, rs.getObject("prize_type_desc"));
                    insert.setObject(5, rs.getObject("place"));
                    insert.setObject(6, rs.getObject("prize_amount"));
                    insert.setObject(7, rs.getObject("prize_desc"));

                    insert.executeUpdate();
                }

            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'contest_prize' table failed.\n" +
                    sqle.getMessage());

        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }

}
