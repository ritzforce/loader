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


public class TCLoadEvent extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadEvent.class);

    @Override
    public void performLoad() throws Exception {
        loadEvent();
        loadEventRegistration();
    }

    public void loadEvent() throws Exception {
        log.info("load event");
        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        ResultSet rs = null;

        try {
            long start = System.currentTimeMillis();

            final String SELECT =
                    "select e.event_desc as event_name, e.event_id " +
                            "from event e where modify_date > ?";

            final String UPDATE = "update event set event_name = ? " +
                    " where event_id = ? ";

            final String INSERT = "insert into event (event_id, event_name) " +
                    "values (?, ?) ";


            select = prepareStatement(SELECT, SOURCE_DB);
            select.setTimestamp(1, fLastLogTime);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            rs = select.executeQuery();

            int count = 0;
            while (rs.next()) {
                count++;
                //log.debug("PROCESSING EVENT " + rs.getInt("event_id"));

                //update record, if 0 rows affected, insert record
                update.clearParameters();
                update.setObject(1, rs.getObject("event_name"));
                update.setLong(2, rs.getLong("event_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    //need to insert
                    insert.clearParameters();
                    insert.setLong(1, rs.getLong("event_id"));
                    insert.setObject(2, rs.getObject("event_name"));

                    insert.executeUpdate();
                }

            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'events' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }

    public void loadEventRegistration() throws Exception {
        log.info("load event registration");
        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        ResultSet rs = null;

        try {
            long start = System.currentTimeMillis();
            final String SELECT = "select er.event_id, er.user_id, er.eligible_ind, er.notes, er.create_date, er.modify_date " +
                    "from event_registration er where er.modify_date > ?";
            final String UPDATE = "update event_registration set eligible_ind = ?, notes = ?, create_date = ?, modify_date = ? " +
                    " where event_id = ? and user_id = ?";

            final String INSERT = "insert into event_registration (event_id, user_id, eligible_ind, notes, create_date, modify_date) " +
                    "values (?, ?, ?, ?, ?, ?) ";


            select = prepareStatement(SELECT, SOURCE_DB);
            select.setTimestamp(1, fLastLogTime);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            rs = select.executeQuery();

            int count = 0;
            while (rs.next()) {
                count++;
                //log.debug("PROCESSING EVENT " + rs.getInt("event_id"));

                //update record, if 0 rows affected, insert record
                update.clearParameters();
                update.setInt(1, rs.getInt("eligible_ind"));
                update.setString(2, rs.getString("notes"));
                update.setTimestamp(3, rs.getTimestamp("create_date"));
                update.setTimestamp(4, rs.getTimestamp("modify_date"));
                update.setLong(5, rs.getLong("event_id"));
                update.setLong(6, rs.getLong("user_id"));


                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    //need to insert
                    insert.clearParameters();
                    insert.setLong(1, rs.getLong("event_id"));
                    insert.setLong(2, rs.getLong("user_id"));
                    insert.setInt(3, rs.getInt("eligible_ind"));
                    insert.setString(4, rs.getString("notes"));
                    insert.setTimestamp(5, rs.getTimestamp("create_date"));
                    insert.setTimestamp(6, rs.getTimestamp("modify_date"));

                    insert.executeUpdate();
                }

            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'event registration' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }




}
