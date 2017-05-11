database tcs_dw;

insert into log_type_lu (log_type_id, log_type_desc) values (4, '');
insert into update_log (log_id, calendar_id, timestamp, log_type_id) values (1, 2859, CURRENT, 4);

INSERT INTO calendar(calendar_id,year,month_numeric,month_alpha,day_of_month,day_of_week,week_day,year_month,week_of_year,day_of_year,holiday,weekend,date,week_year,quarter_of_year) VALUES (20000, 2016, 6, 'June', 27, 1, 'Monday', '2016-6', 27, 285, 'N', 'N', '2016-06-27', 2016, 2);
