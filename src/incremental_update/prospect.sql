SELECT COUNT(*) into source_rows_count from master.prospect;

insert into master.prospect

with date_record_id as (
		select
		dd.sk_dateid
		from master.dimdate dd
		inner join staging.batchdate bd
			on dd.datevalue = bd.batchdate
	)

	select
	  p.agencyid
	, dri.sk_dateid
	, dri.sk_dateid
	, 2 as batchid
	, false -- temporary before dimcustomer load dependency
	, p.lastname
	, p.firstname
	, p.middleinitial
	, p.gender
	, p.addressline1
	, p.addressline2
	, p.postalcode
	, p.city
	, p.state
	, p.country
	, p.phone
	, p.income
	, p.numbercars
	, p.numberchildren
	, p.maritalstatus
	, p.age
	, p.creditrating
	, p.ownorrentflag
	, p.employer
	, p.numbercreditcards
	, p.networth
	, nullif(btrim(btrim(btrim(btrim(btrim(
	  case
		when p.networth > 1000000 or p.income > 200000
		then 'HighValue'
		else ''
	  end
	  || '+' ||
	  case
		when p.numberchildren > 3 or p.numbercreditcards > 5
		then 'Expenses'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.age > 45
		then 'Boomer'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.income < 50000 or p.creditrating < 600 or p.networth < 100000
		then 'MoneyAlert'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.numbercars > 3 or p.numbercreditcards > 7
		then 'Spender'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.age < 25 and p.networth > 1000000
		then 'Inherited'
		else ''
	  end
	  , '+'), '')
	from staging.prospect p
	cross join date_record_id dri
  on conflict (agencyid) do update
    set
        sk_updatedateid = EXCLUDED.sk_updatedateid,
        batchid = EXCLUDED.batchid,
        iscustomer = EXCLUDED.iscustomer,
        lastname = EXCLUDED.lastname,
        firstname = EXCLUDED.firstname,
        middleinitial = EXCLUDED.middleinitial,
        gender = EXCLUDED.gender,
        addressline1 = EXCLUDED.addressline1,
        addressline2 = EXCLUDED.addressline2,
        postalcode = EXCLUDED.postalcode,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        country = EXCLUDED.country,
        phone = EXCLUDED.phone,
        income = EXCLUDED.income,
        numbercars = EXCLUDED.numbercars,
        numberchildren = EXCLUDED.numberchildren,
        maritalstatus = EXCLUDED.maritalstatus,
        age = EXCLUDED.age,
        creditrating = EXCLUDED.creditrating,
        ownorrentflag = EXCLUDED.ownorrentflag,
        employer = EXCLUDED.employer,
        numbercreditcards = EXCLUDED.numbercreditcards,
        networth = EXCLUDED.networth,
        marketingnameplate = EXCLUDED.marketingnameplate;

RETURNING count(*) into insert_rows_count;
