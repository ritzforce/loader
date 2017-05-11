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


public class TCLoadUserContestPrize extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadUserContestPrize.class);

    @Override
    public void performLoad() throws Exception {
        doLoadUserContestPrize();
    }

    private void doLoadUserContestPrize() throws Exception {
        log.info("load user contest prize");
        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;
        ResultSet rs = null;

        try {
            long start = System.currentTimeMillis();
            final String SELECT =
                    "select ucp.user_id, " +
                            "cp.contest_id, " +
                            "cp.prize_type_id, " +
                            "cp.prize_desc as prize_description, " +
                            "cp.place, " +
                            "cp.prize_amount, " +
                            "ucp.payment as prize_payment " +
                            "from user_contest_prize ucp, " +
                            "contest_prize cp, " +
                            "prize_type_lu pt " +
                            "where cp.contest_prize_id = ucp.contest_prize_id " +
                            "and pt.prize_type_id = cp.prize_type_id " +
                            "and (cp.modify_date > ?) ";
            final String INSERT = "insert into user_contest_prize (contest_id, user_id, prize_type_id, " +
                    "prize_description, place, prize_amount, prize_payment) " +
                    "values (?, ?, ?, ?, ?, ?, ?) ";

            final String UPDATE = "update user_contest_prize set prize_type_id = ?,  prize_description = ?, " +
                    "place = ?, prize_amount = ?, prize_payment = ? " +
                    " where contest_id = ? and user_id = ?";

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
                update.setObject(1, rs.getObject("prize_type_id"));
                update.setObject(2, rs.getObject("prize_description"));
                update.setObject(3, rs.getObject("place"));
                update.setObject(4, rs.getObject("prize_amount"));
                update.setObject(5, rs.getObject("prize_payment"));
                update.setLong(6, rs.getLong("contest_id"));
                update.setLong(7, rs.getLong("user_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();
                    insert.setLong(1, rs.getLong("contest_id"));
                    insert.setLong(2, rs.getLong("user_id"));
                    insert.setObject(3, rs.getObject("prize_type_id"));
                    insert.setObject(4, rs.getObject("prize_description"));
                    insert.setObject(5, rs.getObject("place"));
                    insert.setObject(6, rs.getObject("prize_amount"));
                    insert.setObject(7, rs.getObject("prize_payment"));

                    insert.executeUpdate();
                }

            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'user_contest_prize' table failed.\n" +
                    sqle.getMessage());

        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }

}
