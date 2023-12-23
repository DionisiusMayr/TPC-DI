insert into master.dimessages
	select
	  now()
	, 2
	, 'Prospect'
	, 'Source rows'
	, 'Status'
	, (SELECT * from source_row_count);

insert into master.dimessages
	select
	  now()
	, 2
	, 'Prospect'
	, 'inserted rows'
	, 'Status'
	, (SELECT * from insert_rows_count);

insert into master.dimessages
	select
	  now()
	, 2
	, 'Prospect'
	, 'Updated rows'
	, 'Status'
	, (SELECT * from update_rows_count);
