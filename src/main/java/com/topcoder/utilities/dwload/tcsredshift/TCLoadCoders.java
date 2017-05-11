/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.*;
import java.util.ArrayList;


public class TCLoadCoders extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadCoders.class);

    @Override
    public void performLoad() throws Exception {
        doLoadCoders();
    }

    /**
     * <p/>
     * Load coders to the DW.
     * </p>
     *
     * @throws Exception if any error occurs
     */
    public void doLoadCoders() throws Exception {
        log.info("load coders");
        PreparedStatement psSel = null;
        PreparedStatement psIns = null;
        PreparedStatement psUpd = null;
        PreparedStatement psSel2 = null;
        StringBuffer query = null;

        ResultSet rs = null;
        ResultSet rs2 = null;
        int count = 0;
        int retVal = 0;

        try {
            // Our select statement
            query = new StringBuffer(100);
            query.append("SELECT c.coder_id ");                  // 1
            query.append("       ,nvl(a.state_code, '') as state_code ");               // 2
            query.append("       ,decode(a.country_code, null, '', lpad(trim(a.country_code), 3, '0')) as country_code "); // 3
            query.append("       ,u.first_name ");               // 4
            query.append("       ,u.last_name ");                // 5
            query.append("       ,nvl(a.address1, '') as  address1");                 // 6
            query.append("       ,nvl(a.address2, '') as address2");                 // 7
            query.append("       ,nvl(a.city, '') as city ");                     // 8
            query.append("       ,nvl(a.zip, '') as  zip");                      // 9
            query.append("       ,u.middle_name ");              // 10
            query.append("       ,u.activation_code ");          // 11
            query.append("       ,c.member_since ");             // 12
            query.append("       ,c.quote ");                    // 13
            query.append("       ,c.language_id ");              // 14
            query.append("       ,c.coder_type_id ");            // 15
            query.append("       ,u.handle ");                   // 16
            query.append("       ,u.status ");                   // 17
            query.append("       ,e.address ");                  // 18
            query.append("       ,decode(c.comp_country_code, null, '', lpad(trim(c.comp_country_code), 3, '0')) as comp_country_code "); // 19
            query.append("       ,u.last_site_hit_date");        // 20
            query.append("       ,u.reg_source");               // 21
            query.append("       ,u.utm_source");               // 22
            query.append("       ,u.utm_medium");               // 23
            query.append("       ,u.utm_campaign");             // 24
            query.append("       ,u.create_date");              // 25
            query.append("       ,hp.phone_number as home_phone");            // 26
            query.append("       ,wp.phone_number as work_phone");            // 27
            query.append("       ,u.modify_date");              // 28
            query.append("       ,img.image_id as image");              // 29
            query.append("       ,u.handle_lower");              // 30
            query.append("       ,u.last_login");              // 31
            query.append("  FROM informixoltp:coder c ");
            query.append("       ,common_oltp:user u ");
            query.append("       ,common_oltp:email e ");
            query.append("       ,outer (common_oltp:user_address_xref x ,common_oltp:address a) ");
            query.append("       ,outer (common_oltp:phone hp) ");
            query.append("       ,outer (common_oltp:phone wp) ");
            query.append("       ,outer (informixoltp:coder_image_xref imgx, informixoltp:image img) ");
            query.append(" WHERE c.coder_id = u.user_id ");
            query.append("   AND u.user_id = e.user_id ");
            query.append("   and e.primary_ind = 1 ");
            query.append("   and a.address_id = x.address_id ");
            query.append("   and a.address_type_id = 2 ");
            query.append("   and hp.user_id = u.user_id and hp.phone_type_id = 2 and hp.primary_ind = 1 ");
            query.append("   and wp.user_id = u.user_id and wp.phone_type_id = 1 and wp.primary_ind = 1  ");
            query.append("   and imgx.coder_id = u.user_id and imgx.image_id = img.image_id and img.image_type_id = 1 and imgx.display_flag = 1");
            query.append("   and x.user_id = u.user_id ");
            query.append("  and u.user_id in ");
            query.append("   ( ");
            query.append("     select c2.coder_id from informixoltp:coder c2 where c2.modify_date > ?   ");
            query.append("     union ");
            query.append("     select x2.user_id from common_oltp:address a2, common_oltp:user_address_xref x2 where a2.modify_date > ?  and a2.address_id = x2.address_id  ");
            query.append("     union ");
            query.append("     select p2.user_id from common_oltp:phone p2 where p2.modify_date  > ?   ");
            query.append("     union ");
            query.append("     select imgx2.coder_id from informixoltp:image m2, informixoltp:coder_image_xref imgx2 where m2.modify_date  > ? and m2.image_id = imgx2.image_id  ");
            query.append("     union ");
            query.append("     select e2.user_id from common_oltp:email e2 where e2.modify_date  > ?   ");
            query.append("     union ");
            query.append("     select u2.user_id from common_oltp:user u2 where u2.modify_date > ?   ");
            query.append("   ) ");

      /*      query.append("   AND NOT EXISTS ");
            query.append("       (SELECT 'pops' ");
            query.append("          FROM user_group_xref ugx ");
            query.append("         WHERE ugx.login_id= c.coder_id ");
            query.append("           AND ugx.group_id = 2000115)");
            query.append("   AND NOT EXISTS ");
            query.append("       (SELECT 'pops' ");
            query.append("          FROM group_user gu ");
            query.append("         WHERE gu.user_id = c.coder_id ");
            query.append("           AND gu.group_id = 13)");*/

            psSel = prepareStatement(query.toString(), SOURCE_DB);

            // Our insert statement
            query = new StringBuffer(100);
            query.append("INSERT INTO tcs_dw.coder ");
            query.append("      (coder_id ");                   // 1
            query.append("       ,state_code ");                // 2
            query.append("       ,country_code ");              // 3
            query.append("       ,first_name ");                // 4
            query.append("       ,last_name ");                 // 5
            query.append("       ,address1 ");                  // 6
            query.append("       ,address2 ");                  // 7
            query.append("       ,city ");                      // 8
            query.append("       ,zip ");                       // 9
            query.append("       ,middle_name ");               // 10
            query.append("       ,activation_code ");           // 11
            query.append("       ,member_since ");              // 12
            query.append("       ,quote ");                     // 13
            query.append("       ,language_id ");               // 14
            query.append("       ,coder_type_id ");             // 15
            query.append("       ,handle ");                    // 16
            query.append("       ,status ");                    // 17
            query.append("       ,email ");                     // 18
            query.append("       ,comp_country_code ");         // 19
            query.append("       ,last_site_hit_date");         // 20
            query.append("       ,reg_source ");                // 21
            query.append("       ,utm_source ");                // 22
            query.append("       ,utm_medium ");                // 23
            query.append("       ,utm_campaign ");              // 24
            query.append("       ,create_date ");              // 25
            query.append("       ,home_phone ");               // 26
            query.append("       ,work_phone ");               // 27
            query.append("       ,modify_date ");              // 28
            query.append("       ,image ");                    // 29
            query.append("       ,handle_lower ");              // 30
            query.append("       ,last_login )");              // 31
            query.append("VALUES (");
            query.append("?,?,?,?,?,?,?,?,?,?,");  // 10
            query.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");  // 31
            psIns = prepareStatement(query.toString(), TARGET_DB);

            // Our update statement
            query = new StringBuffer(100);
            query.append("UPDATE tcs_dw.coder ");
            query.append("   SET state_code = ? ");                 // 1
            query.append("       ,country_code = ? ");              // 2
            query.append("       ,first_name = ? ");                // 3
            query.append("       ,last_name = ? ");                 // 4
            query.append("       ,address1 = ? ");                  // 5
            query.append("       ,address2 = ? ");                  // 6
            query.append("       ,city = ? ");                      // 7
            query.append("       ,zip = ? ");                       // 8
            query.append("       ,middle_name = ? ");               // 9
            query.append("       ,activation_code = ? ");           // 10
            query.append("       ,member_since = ? ");              // 11
            query.append("       ,quote = ? ");                     // 12
            query.append("       ,language_id = ? ");               // 13
            query.append("       ,coder_type_id = ? ");             // 14
            query.append("       ,handle = ? ");                    // 15
            query.append("       ,status = ? ");                    // 16
            query.append("       ,email = ? ");                     // 17
            query.append("       ,comp_country_code = ?");          // 18
            query.append("       ,last_site_hit_date = ?");         // 19
            query.append("       ,reg_source = ?");                 // 20
            query.append("       ,utm_source = ?");                 // 21
            query.append("       ,utm_medium = ?");                 // 22
            query.append("       ,utm_campaign = ?");               // 23
            query.append("       ,create_date = ?");                // 24
            query.append("       ,home_phone = ?");                 // 25
            query.append("       ,work_phone = ?");                 // 26
            query.append("       ,modify_date = ?");                // 27
            query.append("       ,image = ?");                      // 28
            query.append("       ,handle_lower = ?");               // 29
            query.append("       ,last_login = ?");                 // 30
            query.append(" WHERE coder_id = ?");                    // 31
            psUpd = prepareStatement(query.toString(), TARGET_DB);

            // Our select statement to determine if a particular row is
            // present or not
            query = new StringBuffer(100);
            query.append("SELECT 'pops' ");
            query.append("  FROM tcs_dw.coder ");
            query.append(" WHERE coder_id = ?");
            psSel2 = prepareStatement(query.toString(), TARGET_DB);

            // The first thing we do is delete the old record prior to inserting the
            // new record. We don't care if this fails or not.
            psSel.setTimestamp(1, fLastLogTime);
            psSel.setTimestamp(2, fLastLogTime);
            psSel.setTimestamp(3, fLastLogTime);
            psSel.setTimestamp(4, fLastLogTime);
            psSel.setTimestamp(5, fLastLogTime);
            psSel.setTimestamp(6, fLastLogTime);
            rs = executeQuery(psSel, "loadCoder");

            while (rs.next()) {
                int coder_id = rs.getInt("coder_id");
                psSel2.clearParameters();
                psSel2.setInt(1, coder_id);
                rs2 = psSel2.executeQuery();

                String state_code = rs.getString("state_code");
                String country_code = rs.getString("country_code");
                String first_name = rs.getString("first_name");
                String last_name = rs.getString("last_name");
                String address1 = rs.getString("address1");
                String address2 = rs.getString("address2");
                String city = rs.getString("city");
                String zip = rs.getString("zip");
                String middle_name = rs.getString("middle_name");
                String activation_code = rs.getString("activation_code");
                java.sql.Timestamp member_since = rs.getTimestamp("member_since");
                String quote = rs.getString("quote");
                int language_id = rs.getInt("language_id");
                int coder_type_id = rs.getInt("coder_type_id");
                String handle = rs.getString("handle");
                String status = rs.getString("status");
                String address = rs.getString("address");
                String comp_country_code = rs.getString("comp_country_code");
                java.sql.Timestamp last_site_hit_date = rs.getTimestamp("last_site_hit_date");
                String reg_source = rs.getString("reg_source");
                String utm_source = rs.getString("utm_source");
                String utm_medium = rs.getString("utm_medium");
                String utm_campaign = rs.getString("utm_campaign");
                java.sql.Timestamp create_date = rs.getTimestamp("create_date");
                String home_phone = rs.getString("home_phone");
                String work_phone = rs.getString("work_phone");
                java.sql.Timestamp modify_date = rs.getTimestamp("modify_date");
                int image = rs.getInt("image");
                String handle_lower = rs.getString("handle_lower");
                java.sql.Timestamp last_login = rs.getTimestamp("last_login");

                // If next() returns true that means this row exists. If so,
                // we update. Otherwise, we insert.
                if (rs2.next()) {
                    psUpd.clearParameters();
                    psUpd.setString(1, state_code);
                    psUpd.setString(2, country_code);
                    psUpd.setString(3, first_name);
                    psUpd.setString(4, last_name);
                    psUpd.setString(5, address1);
                    psUpd.setString(6, address2);
                    psUpd.setString(7, city);
                    psUpd.setString(8, zip);
                    psUpd.setString(9, middle_name);
                    psUpd.setString(10, activation_code);
                    psUpd.setTimestamp(11, member_since);
                    psUpd.setString(12, quote);
                    psUpd.setInt(13, language_id);
                    psUpd.setInt(14, coder_type_id);
                    psUpd.setString(15, handle);
                    psUpd.setString(16, status);
                    psUpd.setString(17, address);
                    psUpd.setString(18, comp_country_code);
                    psUpd.setTimestamp(19, last_site_hit_date);
                    psUpd.setString(20, reg_source);
                    psUpd.setString(21, utm_source);
                    psUpd.setString(22, utm_medium);
                    psUpd.setString(23, utm_campaign);
                    psUpd.setTimestamp(24, create_date);
                    psUpd.setString(25, home_phone);
                    psUpd.setString(26, work_phone);
                    psUpd.setTimestamp(27, modify_date);
                    psUpd.setInt(28, image);
                    psUpd.setString(29, handle_lower);
                    psUpd.setTimestamp(30, last_login);
                    psUpd.setLong(31, coder_id);

                    // Now, execute the update of the new row
                    try {
                        retVal = psUpd.executeUpdate();
                    } catch(SQLException e) {
                      log.error("Failed to load coder {coder_id:"+coder_id+", state_code:"+state_code+", country_code:"+country_code+", first_name:"+first_name+", last_name:"+last_name+", address1:"+address1+", address2:"+address2+", city:"+city+", zip:"+zip+", middle_name:"+middle_name+", activation_code:"+activation_code+", member_since:"+member_since+", quote:"+quote+", language_id:"+language_id+", coder_type_id:"+coder_type_id+
                                ", handle:"+handle+", status:"+status+", address:"+address+", comp_country_code:"+comp_country_code+", last_site_hit_date:"+last_site_hit_date+", reg_source:"+reg_source+", utm_source:"+utm_source+", utm_medium:"+utm_medium+", utm_campaign:"+utm_campaign+", create_date:"+create_date+", home_phone:"+home_phone+", work_phone:"+work_phone+", modify_date:"+modify_date+", image:"+image+", handle_lower:"+handle_lower+
                                ", last_login:"+last_login+"}");
                        throw e;
                    }
                    count = count + retVal;
                    if (retVal != 1) {
                        throw new SQLException("TCLoadCoders: Update for coder_id " +
                                coder_id +
                                " modified " + retVal + " rows, not one.");
                    }
                } else {
                    psIns.clearParameters();
                    psIns.setInt(1, coder_id);
                    psIns.setString(2, state_code);
                    psIns.setString(3, country_code);
                    psIns.setString(4, first_name);
                    psIns.setString(5, last_name);
                    psIns.setString(6, address1);
                    psIns.setString(7, address2);
                    psIns.setString(8, city);
                    psIns.setString(9, zip);
                    psIns.setString(10, middle_name);
                    psIns.setString(11, activation_code);
                    psIns.setTimestamp(12, member_since);
                    psIns.setString(13, quote);
                    psIns.setInt(14, language_id);
                    psIns.setInt(15, coder_type_id);
                    psIns.setString(16, handle);
                    psIns.setString(17, status);
                    psIns.setString(18, address);
                    psIns.setString(19, comp_country_code);
                    psIns.setTimestamp(20, last_site_hit_date);
                    psIns.setString(21, reg_source);
                    psIns.setString(22, utm_source);
                    psIns.setString(23, utm_medium);
                    psIns.setString(24, utm_campaign);
                    psIns.setTimestamp(25, create_date);
                    psIns.setString(26, home_phone);
                    psIns.setString(27, work_phone);
                    psIns.setTimestamp(28, modify_date);
                    psIns.setInt(29, image);
                    psIns.setString(30, handle_lower);
                    psIns.setTimestamp(31, last_login);

                    // Now, execute the insert of the new row
                    try {
                        retVal = psIns.executeUpdate();
                    } catch(SQLException e) {
                        log.error("Failed to load coder {coder_id:"+coder_id+", state_code:"+state_code+", country_code:"+country_code+", first_name:"+first_name+", last_name:"+last_name+", address1:"+address1+", address2:"+address2+", city:"+city+", zip:"+zip+", middle_name:"+middle_name+", activation_code:"+activation_code+", member_since:"+member_since+", quote:"+quote+", language_id:"+language_id+", coder_type_id:"+coder_type_id+
                                  ", handle:"+handle+", status:"+status+", address:"+address+", comp_country_code:"+comp_country_code+", last_site_hit_date:"+last_site_hit_date+", reg_source:"+reg_source+", utm_source:"+utm_source+", utm_medium:"+utm_medium+", utm_campaign:"+utm_campaign+", create_date:"+create_date+", home_phone:"+home_phone+", work_phone:"+work_phone+", modify_date:"+modify_date+", image:"+image+", handle_lower:"+handle_lower+
                                  ", last_login:"+last_login+"}");
                        throw e;
                    }
                    count = count + retVal;
                    if (retVal != 1) {
                        throw new SQLException("TCLoadCoders: Insert for coder_id " +
                                coder_id +
                                " modified " + retVal + " rows, not one.");
                    }
                }

                close(rs2);
                printLoadProgress(count, "coder");
            }

            log.info("Coder records updated/inserted = " + count);
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'coder' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(rs2);
            close(psSel);
            close(psIns);
            close(psSel2);
        }
    }

    private void setTimestampParameter(ResultSet rs, PreparedStatement statement, String name, int index) throws SQLException {
        if (rs.getObject(name) != null) {
            statement.setTimestamp(index, rs.getTimestamp(name));
        } else {
            statement.setNull(index, Types.TIMESTAMP);
        }
    }

    private void setLongParameter(ResultSet rs, PreparedStatement statement, String name, int index) throws SQLException {
        if (rs.getObject(name) != null) {
            statement.setLong(index, rs.getLong(name));
        } else {
            statement.setNull(index, Types.DECIMAL);
        }
    }

}
