
Proc sql feedback;
%snwflk;
create table Waterfall as select * from connection to snwflk
(Select
s.uai_id
		   ,s.acct_id
,case when trim(s.ASif_Attribute) = 'Y' then '0015'
when trim(s.PIFSIF_Flag) = 'Y' then '0014'
when trim(s.BK_Flag) = 'Y' then '0013'
when trim(s.Lit_Flag) = 'Y' then '0012'
when trim(s.Repo_Flag) = 'Y' then '0011'
when trim(s.Deceased_Flag) = 'Y' then '0010'
when trim(s.AR_Flag) = 'Y' then '0009'
when trim(s.CD_Flag) = 'Y' then '0008'
when trim(s.Active_Complaint_Flag) = 'Y' then '0007'
when trim(s.CCCS_Flag) = 'Y' then '0006'
when trim(s.Wisc128_Flag) = 'Y' then '0005'
when trim(s.Dispute_Flag) = 'Y' then '0004'
when trim(s.Fraud_Flag) = 'Y' then '0003'
when trim(s.Mod_Attribute) = 'Y' or trim(s.mod_attribute) = 'Y' then '0002'
					  when trim(s.a3p_dsa_flag) = 'Y' then '0016'
when trim(s.confidential_Flag) = 'Y' then '0001'
else '0000' end ::varchar(4) as Waterfall
from
(Select
c.*
,BK.BK_Attribute
,Complaint.Complaint_Attribute
,CD.CD_Attribute
,Fraud.Fraud_Attribute
,PifSif.PifSif_Attribute
			  ,ASif.ASif_Attribute
,AR.AR_Attribute
,Lit.LIT_Attribute
,CCCS.CCCS_Attribute
,Deceased.Deceased_Attribute
,Modatt.Mod_Attribute
,conf.confidential_Flag
,case when trim(BK.BK_Attribute) = 'Y'
or trim(c.chapt7_yn) = 'Y'
or trim(c.chapt11_yn) = 'Y'
or trim(c.chapt13_yn) = 'Y'
then 'Y' else 'N' end as BK_Flag
,case when trim(Complaint.Complaint_Attribute) = 'Y' then 'Y' else 'N' end as Active_Complaint_Flag
,case when trim(c.judgement_yn) = 'Y'
or trim(Lit.LIT_Attribute) = 'Y'
then 'Y' else 'N' end as Lit_Flag
,case when trim(PifSif.PifSif_Attribute) = 'Y'
or trim(c.acct_status) = 'P'
then 'Y' else 'N' end as PIFSIF_Flag
,case when trim(CD.CD_Attribute) = 'Y'
then 'Y' else 'N' end as CD_Flag
,case when trim(Fraud.Fraud_Attribute) = 'Y'
then 'Y' else 'N' end as Fraud_Flag
,case when trim(AR.AR_Attribute) = 'Y'
then 'Y' else 'N' end as AR_Flag
,case when trim(c.cccs_yn) = 'Y'
or trim(CCCS.CCCS_Attribute) = 'Y'
then 'Y' else 'N' end as CCCS_Flag
,case when trim(Deceased.Deceased_Attribute) = 'Y'
then 'Y' else 'N' end as Deceased_Flag
,case when trim(wisc.Wisc128_Flag) = 'Y' then 'Y' else 'N' end as Wisc128_Flag
			   ,dsa.a3p_dsa_flag
from
(Select
a.*
,case when trim(b.Repo_Attribute) = 'Y' then 'Y' else 'N' end::Varchar(1) as Repo_Flag
from
(Select
Uai_Id
,acct_id
,score_br as branch_n
,Acct_Status
,chgoff_date
,case when chgoff_date is not null or trim(acct_status) = 'C' then 'Y' else 'N' end as Charge_Off_Flag
,case when trim(prod_line) between 1100 and 1199 then 'PHL' else 'PUL' end as prod_type
,chapt7_yn
,chapt11_yn
,chapt13_yn
,judgement_yn
,repo_yn
,cccs_yn
,fraud_yn
,in_dispute_yn
,case when trim(in_dispute_yn) = 'Y' then 'Y' else 'N' end as Dispute_Flag
from ods.sat_account_daily_management_report_Detail
where period_end_dt = (current_date - 1)) as a
left outer join
(Select distinct
uai_id
,'Y' as Repo_Attribute
from ods.sat_account_cqs_attribute
where upper(attribute_desc)::varchar like '%REPOSSESSION%'
and trim(attribute_name) NOT IN  ('REPOTURNDN','REPOREDEEM', 'REPOSOLD  ')) as b
on trim(a.uai_id) = trim(b.uai_id)) as c
left outer join
(Select Distinct
uai_id
,'Y' as BK_Attribute
from  ods.sat_account_cqs_attribute
where upper(attribute_desc)::varchar like '%BANK%'
and attribute_name::varchar not like ('%SCRUBFULL%')) as BK
on trim(c.uai_id) = trim(bk.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as Complaint_Attribute
from  ods.sat_account_cqs_attribute
where upper(attribute_desc)::varchar like '%ACTIVE COMPLAINT%') as Complaint
on trim(c.uai_id) = trim(Complaint.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as CD_Attribute
from  ods.sat_account_cqs_attribute
where upper(attribute_name)::varchar like 'CEASE_DST%'
or attribute_name::varchar like '%CANDDREV%'  ) as CD
on trim(c.uai_id) = trim(CD.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as Fraud_Attribute
from  ods.sat_account_cqs_attribute
where upper(attribute_desc)::varchar like '%FRAUD%') as Fraud
on trim(c.uai_id) = trim(Fraud.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as PifSif_Attribute
from  ods.sat_account_cqs_attribute
where (upper(attribute_desc)::varchar like '%PIF%'
or upper(attribute_desc)::varchar like '%SIF%')) as PifSif
on trim(c.uai_id) = trim(PifSif.uai_id)
left outer join
(Select Distinct
uai_id
,active_sif_i as ASif_Attribute
From  ods.sat_account_daily_class_extension
Where trim(active_sif_i) in ('Y')
							 and ods_update_dt = (current_date)) as ASif
on trim(c.uai_id) = trim(ASif.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as AR_Attribute
from  ods.sat_account_cqs_attribute
where upper(attribute_desc)::varchar like 'ATTO%'
or attribute_name::varchar like '%ARREVIEW%') as AR
on trim(c.uai_id) = trim(AR.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as LIT_Attribute
from  ods.sat_account_cqs_attribute
where (upper(attribute_desc)::varchar like '%LITIGATION%'
and upper(attribute_name)::varchar not like '%LITTURNDN%')) as LIT
on trim(c.uai_id) = trim(LIT.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as CCCS_Attribute
from  ods.sat_account_cqs_attribute
where upper(attribute_desc)::varchar like '%CCCS%') as CCCS
on trim(c.uai_id) = trim(CCCS.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as Mod_Attribute
from  ods.sat_account_cqs_attribute
where trim(upper(attribute_name)) in ('M0','M1','M2','M3','M4'/*,'M5*/)) as Modatt
on trim(c.uai_id) = trim(Modatt.uai_id)
left outer join
(Select Distinct
uai_id
,'Y' as Deceased_Attribute
from  ods.sat_account_cqs_attribute
where upper(attribute_desc)::varchar like '%DECEASED%'
or attribute_name::varchar like '%DECREVIEW%') as Deceased
on trim(c.uai_id) = trim(Deceased.uai_id)
Left outer join
(Select distinct
b.uai_id
,'Y' as confidential_Flag
from
(Select
account_key
,adv_rcpt_cd
from ods.sat_account_daily_management_report_detail
where period_end_dt = date(date_trunc('month', current_date) - Interval '1 Day')
and trim(adv_rcpt_cd) = 'C') as a
inner join
(Select
uai_id
,account_key
from ods.sat_account_daily_management_report_detail
where period_end_dt = date(date_trunc('month', current_date) - Interval '1 Day')) as b
on trim(a.account_key) = trim(b.account_key)) as conf
on trim(c.uai_id) = trim(conf.uai_id)
Left outer join
(Select
uai_id
,wisconsin_128_i as Wisc128_Flag
from ods.sat_account_daily_class_master
where trim(wisconsin_128_i) = 'Y'
and load_dt = current_date -1 ) as wisc
on trim(c.uai_id) = trim(wisc.uai_id)
 
		Left Outer Join
 
						(select
					          x.account_n
					        ,x.branch_n
					        ,x.a3p_dsa_flag
							,x.a3p_name::varchar(100) as DSA_Name
					        ,date(x.chg_timestamp) as Dsa_Date
							,x.a3p_received_date
							,x.a3p_expiration_date
							,date(x.add_timestamp) as add_Date
							,date(x.dsaovr_timestamp) as Dsa_Ovrrde_Date
							,x.dsa_rev_ovr_date as Override_Expiration_Date
							,x.a3p_poa_flag     /*Added a3p_poa_flag on 10/27/2017: Tanuja*/
							
 
						    	from  dialer.csa3pt00 as x
 
						inner join
 
							(select
								account_n
					           ,max(chg_timestamp) as max
 
					            from dialer.csa3pt00
 
								where trim(a3p_dsa_flag) = 'Y'
 
					            group by account_n) as y
 
							on  x.account_n = y.account_n and x.chg_timestamp = y.max
 
						where trim(x.a3p_dsa_flag) = 'Y') as Dsa
 
				on trim(c.acct_id) = trim(dsa.account_n)) as s
);quit;
 
