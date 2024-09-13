with daa as (select  --a.awb,
		case when b.city_name in ('Bogor','Depok','Tangerang','Bekasi') 
				or b.province_name='DKI Jakarta' then 'Jabodetabek'
			else b.city_name end as city_name,
--			case when c.city_name in ('Bogor','Depok','Tangerang','Bekasi') 
--				or c.province_name='DKI Jakarta' then 'Jabodetabek'
--			else c.city_name end as city_name_origin
			case when a.payment_type =3 then 'COD'
			when a.payment_type <> 3 then 'NON_COD' end as flag,
			doo.created_timestamp,
			a.order_time,
			a."54_tm",
			a."70_tm",
			a."80_tm",
			coalesce((extract(epoch from (a.order_time  - doo.created_timestamp))), 0) as os,
			coalesce((extract(epoch from (a."54_tm" - a.order_time))), 0) as sp,
			coalesce((extract(epoch from (a."54_tm" - doo.created_timestamp))), 0) as op,
			coalesce((extract(epoch from (a."80_tm" - doo.created_timestamp))), 0) as _80a,
			coalesce((extract(epoch from (a."80_tm" - a."54_tm"))), 0) as _8054
from analysis.summary a 
inner join analysis.unitzone_v2 b 
					on a.delivery_unitzone =b.receive_unitarea and b.flag_pud=0 
					and b.city_code in ('32.73','32.04','35.73','35.07','33.22','33.74','35.78','34.71','32.01','32.71','32.76','36.71','36.03','32.16','32.75','31.73','31.71','31.74','31.75','31.72')
inner join analysis.unitzone_v2 c 
					on a.pickup_unitzone =c.receive_unitarea and c.flag_pud=1
					and c.city_code in ('32.73','32.04','35.73','35.07','33.22','33.74','35.78','34.71','32.01','32.71','32.76','36.71','36.03','32.16','32.75','31.73','31.71','31.74','31.75','31.72')
--left join analysis_services.tb_mst_area_by_province d on b.city_code =d.city_code
--left join analysis_services.tb_mst_area_by_province e on c.city_code =e.city_code
left join doit.doit_orders doo on a.awb =doo.waybill_no 
					and created_timestamp >= now()::date - interval '3 month' and created_timestamp < now()::date
where a.order_time_dt >= now()::date-interval ' 3 month'
	and a.order_time_dt < now()::date-interval '0 day' 
	and a."80_tm" >= '${STARTDATE}'--now()::date-interval '7 day'
	and a."80_tm" < '${ENDDATE}'--now()::date-interval '0 day' 
	and customer_code ='0000000001'
	AND a.service_code ='REG'
	and b.city_name = c.city_name ),
	--group by 1,2,3,4,5,6,7 ) ,
percentile as( select 
		a.city_name, avg(_8054) ,
		PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (_8054::numeric))
                     FILTER (WHERE _8054 > 0)::NUMERIC(10, 2) AS percentile_8054,
                     avg(_80a),
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY _80a::numeric)
					FILTER (WHERE _80a > 0)::NUMERIC(10, 2) AS percentile_80a
	from daa a group by 1)	--select * from percentile;
	select a.city_name,
		FLoor(avg(_80a)::numeric / 3600) || ':' || 
		    LPAD(FLOOR((avg(_80a)::numeric % 3600) / 60)::TEXT, 2, '0') || ':' || 
		    LPAD(floor((avg(_80a))::numeric % 60)::TEXT, 2, '0')					as "created_timestamp to actual_delivered_timestamp (avg) hours",
		FLoor(avg(op)::numeric / 3600) || ':' || 
		    LPAD(FLOOR((avg(op)::numeric % 3600) / 60)::TEXT, 2, '0') || ':' || 
		    LPAD(floor((avg(op))::numeric % 60)::TEXT, 2, '0') 						as "created_timestamp to actual_pickup_timestamp (avg) hours",
		FLoor(avg(os)::numeric / 3600) || ':' || 
		    LPAD(FLOOR((avg(os)::numeric % 3600) / 60)::TEXT, 2, '0') || ':' || 
		    LPAD(floor((avg(os))::numeric % 60)::TEXT, 2, '0') 						as "created_timestamp to request_order_timestamp (avg) hours",
		FLoor(avg(sp)::numeric / 3600) || ':' || 
		    LPAD(FLOOR((avg(sp)::numeric % 3600) / 60)::TEXT, 2, '0') || ':' || 
		    LPAD(floor((avg(sp))::numeric % 60)::TEXT, 2, '0') 					as "request_order_timestamp to actual_pickup_timestamp (avg) hours",
		FLoor(avg(_8054)::numeric / 3600) || ':' || 
		    LPAD(FLOOR((avg(_8054)::numeric % 3600) / 60)::TEXT, 2, '0') || ':' || 
		    LPAD(floor((avg(_8054))::numeric % 60)::TEXT, 2, '0') 					as "actual_pickup_timestamp to actual_delivered_timestamp (avg) hours",
		FLoor(b.percentile_80a::numeric / 3600) || ':' || 
		    LPAD(FLOOR((b.percentile_80a::numeric % 3600) / 60)::TEXT, 2, '0') || ':' || 
		    LPAD(floor(b.percentile_80a::numeric % 60)::TEXT, 2, '0')		as  "created_timestamp to actual_delivered_timestamp (p95) hours",
		--b.percentile_80a,    
    	round(count(*) filter (where (_8054)::numeric > b.percentile_8054::numeric)*100.0/count (*) filter (where a."80_tm">'1990-01-01'),2)  			as "longtail order %",
		round(count(*) filter (where a.flag ='COD' and a."70_tm">'1990-01-01') *100.0/count(*) filter (where a."80_tm">'1990-01-01'),2)					as "COD Failed Delivery rate %" ,
		round(count(*) filter (where a.flag ='NON_COD' and a."70_tm">'1990-01-01') *100.0/count(*) filter (where a."80_tm">'1990-01-01'),2)				as "Non COD Failed Delivery rate %"
	from  daa a
	left join percentile b on a.city_name =b.city_name
	group by a.city_name, percentile_80a;