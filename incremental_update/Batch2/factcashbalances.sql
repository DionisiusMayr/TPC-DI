-- factcashbalances
truncate table master.factcashbalances;
insert into master.factcashbalances
	with agg as (
		select 
		a.sk_customerid as sk_customerid,
		a.sk_accountid as sk_accountid,
		d.sk_dateid as sk_dateid,
		sum(ct_amt) as ct_amt_day
		from staging.cashtransaction c,
			master.dimaccount a,
			master.dimdate d
		where c.ct_ca_id = a.accountid
    and a.iscurrent= True
    and TO_DATE(d.sk_dateid::text, 'YYYYMMDD')= (SELECT batchdate from staging.batchdate); 
		group by 
			a.sk_customerid,
			a.sk_accountid,
			d.sk_dateid
	)

	, final_output as (
		select
		sk_customerid,
		sk_accountid,
		sk_dateid,
		sum(ct_amt_day) over(partition by sk_accountid order by sk_dateid rows between unbounded preceding and current row) as cash,
		2 as batchid
		from agg
	)

	select * from final_output;

-- select sk_accountid, sum(cash) 
-- from master.factcashbalances 
-- group by (sk_customerid, sk_accountid, sk_dateid);
