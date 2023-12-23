-- DECLARE 
--     update_rows_count INT;

with current_active_customer as (
	select p.*
	from master.prospect p
	inner join master.dimcustomer c
	on upper(c.lastname) = upper(p.lastname)
	and upper(c.firstname) = upper(p.firstname)
	and upper(c.addressline1) = upper(p.addressline1)
	and upper(c.addressline2) = upper(p.addressline2)
	and upper(c.postalcode) = upper(p.postalcode)
	where c.status = 'ACTIVE'
	and c.iscurrent = true
),

prospect_prior as (
    SELECT
        agencyid,
        lastname,
        firstname,
        middleinitial,
        gender,
        addressline1,
        addressline2,
        postalcode,
        city,
        state,
        country,
        phone,
        income,
        numbercars,
        numberchildren,
        maritalstatus,
        age,
        creditrating,
        ownorrentflag,
        employer,
        numbercreditcards,
        networth
    FROM prospect
    WHERE sk_updatedateid IS NOT NULL
),

batchdate as(
  select batchdate from staging.batchdate
)

update master.prospect
	set iscustomer = true 
	where lastname in (select lastname from current_active_customer)
	and firstname in (select firstname from current_active_customer)
	and addressline1 in (select addressline1 from current_active_customer)
	and addressline2 in (select addressline2 from current_active_customer)
	and postalcode in (select postalcode from current_active_customer);

update master.prospect as prospect
set sk_updatedateid = 
    CASE
        WHEN sk_updatedateid IS NULL OR NOT EXISTS (
            SELECT 1
            FROM prospect_prior
            WHERE prospect_prior.agencyid = prospect.agencyid
        )
        THEN batchdate
        WHEN EXISTS (
            SELECT 1
            FROM prospect_prior
            WHERE prospect_prior.agencyid = prospect.agencyid
              AND (
                prospect_prior.lastname != prospect.lastname
                OR prospect_prior.firstname != prospect.firstname
                OR prospect_prior.middleinitial != prospect.middleinitial
                OR prospect_prior.gender != prospect.gender
                OR prospect_prior.addressline1 != prospect.addressline1
                OR prospect_prior.addressline2 != prospect.addressline2
                OR prospect_prior.postalcode != prospect.postalcode
                OR prospect_prior.city != prospect.city
                OR prospect_prior.state != prospect.state
                OR prospect_prior.country != prospect.country
                OR prospect_prior.phone != prospect.phone
                OR prospect_prior.income != prospect.income
                OR prospect_prior.numbercars != prospect.numbercars
                OR prospect_prior.numberchildren != prospect.numberchildren
                OR prospect_prior.maritalstatus != prospect.maritalstatus
                OR prospect_prior.age != prospect.age
                OR prospect_prior.creditrating != prospect.creditrating
                OR prospect_prior.ownorrentflag != prospect.ownorrentflag
                OR prospect_prior.employer != prospect.employer
                OR prospect_prior.numbercreditcards != prospect.numbercreditcards
                OR prospect_prior.networth != prospect.networth
              )
        )
        THEN batchdate
        ELSE sk_updatedateid
    END
FROM master.dimdate
WHERE master.dimdate.sk_dateid = prospect.sk_updatedateid;

RETURNING count(*) into update_rows_count;