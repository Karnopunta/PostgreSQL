-- DROP FUNCTION b2b.proc_aware_get_report_b2b_awbstatus_v1(text, text, text, text, text, text, text, text, text, text, text, text, int4, int4);

CREATE OR REPLACE FUNCTION b2b.proc_aware_get_report_b2b_awbstatus_v1(p_typelist text, p_awb text, p_startdate text, p_enddate text, p_latestopcode_internal text, p_latestopcode_external text, p_limit text, p_page text, p_sortactive text, p_sortdirection text, p_nik text, p_report_name text, p_download integer, p_distribution_type integer)
 RETURNS TABLE(awb text, kode_booking text, invoice_no text, service_code text, order_time text, expected_pickup_tm text, _54_tm text, pickup_sla text, expect_delivery_tm text, _80_tm text, delivery_sla text, shipper_name text, shipper_address text, shipper_contact text, receiver_name text, receiver_address text, receiver_phone text, origin_city text, origin_uz text, ori_city_code_shipper text, origin_staging text, dest_city text, destination_uz text, dest_city_code_receiver text, receiver_geoloc text, destination_staging text, zone_code_internal text, time_scan_internal text, kode_pegawai_internal text, nama_operator_internal text, jabatan_internal text, hub_operator_internal text, staging_operator_internal text, fop_code_internal text, fop_name_internal text, _100_tm text, _100_id text, _101_id text, _60_tm text, _54_id text, jabatan_54 text, _44_tm text, _641_tm text, _op204_tm text, _204_staging text, _op205_tm text, _205_id text, _205_staging text, _70_tm text, _209_last text, _205_last text, _80_last text, _80_id text, _80_id_staging text, opcode201 text, opcode206 text, opcode55 text, _90_tm text, latest_33 text, tm_92 text, op_59 text, op_56 text, why_code_60 text, _60_why_code_last text, why_note_last_60 text, _70_why_code text, _70_why_code_last text, _70_why_notes_last text, why_code_33 text, why_code_92 text, why_notes_92 text, bagseal_no text, attempt60 text, attempt70 text, _70_tm_first text, _70_why_code_first text, return_awb text, etd_returned text, latest_op_5 text, latest_scan_time_5 text, latest_scan_op_5 text, latest_hub_5 text, latest_staging_5 text, _80_tm_5 text, flag_pod text, consignee text, item_name text, item_category text, flag_cod text, cod_amount text, merchant_weight text, actual_weight text, fee_amt_ongkir text, parcel_total_weight text, harga_barang text, insurance text, delivery_due_edited text, delivery_sla_edited text, "3PL" text, "Live Resi" text, "Start Return" text, "Done Return" text, autoclaim_due text, channel text)
 LANGUAGE plpgsql
AS $function$
declare
    /*
        -- Created by karno,
        ---------------------------------------------------------------------------------------------------------
        PIC	   DATE	                NOTES
        ---------------------------------------------------------------------------------------------------------
        KAR 12/09/2024 (create new fungtion)

awb_source := 'WITH ';
awb_source_query := '')
     KAR 12/09/2024 ( update source
     
     */


    /* call sp :
        select *
        from b2b.proc_aware_get_report_b2b_awbstatus_v1('AWB',
                                                               '''11000530660725'',''10006578973586'',''10006571565407'',''10006573583702''',
                                                               '2024-07-01', '2024-09-12', '', '', '', '', '', '',
                                                               '', '', '2', '2');
     */

    v_typelist                   text := null;
    v_awb                        text := null;
    v_start_date                 text := null;
    v_end_date                   text := null;
    v_limit                      text := null;
    v_offset                     text := null;
    v_order_by                   text := null;
    v_orderdirection             text := null;
    v_latest_opcode_internal     text := null;
    v_latest_opcode_external     text := null;
    v_invoice                    text;
    base_query                   text;
    awb_source                   text;
    awb_query                    text;
    awb_source_query             text;
    invoice_query                text;
    startDate_query              text;
    endDate_query                text;
    startreturn_query            text;
    endreturn_query              text;
    latest_opcode_internal_query text;
    latest_opcode_external_query text;
    closing_query                text;
    final_query                  text;
    limit_query                  text;
    offset_query                 text;
    orderBy_query                text;
    par_json                     json := null;
    v_report_id                  int;
    v_group                      text;
BEGIN
    --     RAISE NOTICE 'JSON : % ', par_json;
    --fill variabel value from json
    v_typelist := p_typelist;
    v_awb := p_awb;
    v_start_date := p_startdate;
    v_end_date := p_enddate;
    v_latest_opcode_internal := p_latestopcode_internal;
    v_latest_opcode_external := p_latestopcode_external;
    v_limit := p_limit;
    v_offset := p_page;
    v_order_by := p_sortactive;
    v_orderdirection := p_sortdirection;

    if v_awb is null or v_awb = ''
    then
        v_awb = '1';
    else
        v_awb = v_awb;
    end if;
	
--	if LENGTH(v_awb) < 10
--	then 
--    	offset_query := '';
--	else
		offset_query := b2b.helper_create_limit_offset(v_offset, v_limit);
--	end if;


    orderBy_query := b2b.f_convert_order_by(v_order_by, v_orderdirection);
    startDate_query :=
            b2b.f_convert_where_parameter_datetime('STARTDATE', 'a.order_time', v_start_date);
    endDate_query := b2b.f_convert_where_parameter_datetime('ENDDATE', 'a.order_time', v_end_date);
    startreturn_query :=
            b2b.f_convert_where_parameter_datetime('STARTDATE', 'awe.order_time', v_start_date);
    endreturn_query := b2b.f_convert_where_parameter_datetime('ENDDATE', 'awe.order_time', v_end_date);
    --    awb_query = analysis_services.f_convert_Where_Parameter_With_EqualOrIn('a.awb_no', v_awb);
--    latest_opcode_internal_query := analysis_services.f_convert_Where_Parameter_With_EqualOrIn('a.fop_code_internal', v_latest_opcode_internal);
--    latest_opcode_external_query := analysis_services.f_convert_Where_Parameter_With_EqualOrIn('a.fop_code_external', v_latest_opcode_external);
    latest_opcode_internal_query :=
            b2b.f_convert_Where_Parameter_With_EqualOrIn('a.fop_code_internal',
                                                                       v_latest_opcode_internal);
    latest_opcode_external_query :=
            b2b.f_convert_Where_Parameter_With_EqualOrIn('a.latest_op_code_external',
                                                                       v_latest_opcode_external);
    CASE upper(v_typelist)
        WHEN 'AWB' THEN awb_query := b2b.f_convert_Where_Parameter_With_EqualOrIn('a.awb', v_awb);
                        awb_source := 'WITH awb AS (SELECT unnest(array[' || v_awb || '])::text AS awb)';
                        awb_source_query := 'and a.awb IN (SELECT * FROM AWB)';
        WHEN 'BOOKING_NO'
            THEN awb_query := b2b.f_convert_Where_Parameter_With_EqualOrIn('a.kode_booking', v_awb);
                 v_awb := CASE WHEN position(',' in v_awb) = 0 then quote_literal(v_awb)::text else v_awb end;
                 awb_source := 'WITH BOOKING_NO AS (SELECT unnest(array[' || v_awb || '])::text AS awb) ';
                 awb_source_query := 'and a.kode_booking IN (SELECT awb FROM BOOKING_NO)';
        WHEN 'INVOICE_NO'
            THEN awb_query := b2b.f_convert_Where_Parameter_With_EqualOrIn('a.invoice_no', v_awb);
				 awb_source := 'WITH INVOICE_NO AS (SELECT unnest(array[' || v_awb || '])::text AS awb) ';
                 awb_source_query := 'and a.invoice_no IN (SELECT awb FROM INVOICE_NO)';
        ELSE awb_query := b2b.f_convert_Where_Parameter_With_EqualOrIn('a.awb', v_awb);
             awb_source := '';
             awb_source_query := '';
        END CASE;
    base_query := 'select 
	a.awb::text 			awb,
	kode_booking::text      kode_booking,
	invoice_no::text 		invoice_no,
	service_code::text 		service_code,
	order_time::text 		order_time,
	expected_pickup_tm::text expected_pickup_tm,
	_54_tm::text			_54_tm,
	''''::text					pickup_sla,
	expect_delivery_tm::text 	expect_delivery_tm,
	_80_tm::text 				_80_tm,
	''''::text 					delivery_sla,
	shipper_name::text 			shipper_name,
	shipper_address::text 		shipper_address,
	shipper_contact::text 		shipper_contact,
	receiver_name::text 		receiver_name,
	receiver_address::text 		receiver_address,
	receiver_phone::text 		receiver_phone,
	d.city_name::text 			origin_city ,
	origin_uz::text 			origin_uz,
	d.city_code::text 			ori_city_code_shipper , 
	--d.joint_point_code::text  ,
	origin_staging::text 		origin_staging,
	e.city_name::text			dest_city ,
	destination_uz::text 		destination_uz,
	e.city_code::text  			dest_city_code_receiver ,
	receiver_geoloc::text 		receiver_geoloc,
	destination_staging::text 	destination_staging,
	zone_code_internal::text 	zone_code_internal,
	time_scan_internal::text	time_scan_internal,
	kode_pegawai_internal::text kode_pegawai_internal,
	nama_operator_internal::text	nama_operator_internal,
	jabatan_internal::text			jabatan_internal,
	hub_operator_internal::text		hub_operator_internal,
	staging_operator_internal::text	staging_operator_internal,
	fop_code_internal::text			fop_code_internal,
case
	WHEN a.fop_code_internal ::text = ''90''::text THEN ''Cancel''::text
    WHEN a.fop_code_internal ::text = ''80''::text THEN ''Delivered''::text
    WHEN a.fop_code_internal::text = ''54''::text THEN ''Driver already picked up the item from seller''::text
    ELSE COALESCE(b.description::text, c.op_name::text)
END       AS          		fop_name_internal,
"_100_tm"::text				_100_tm,
_100_id::text				_100_id,
_101_id::text				_101_id,
_60_tm::text				_60_tm,
"54_id"::text				"54_id",
jabatan_54::text			jabatan_54,
_44_tm::text				_44_tm,
_641_tm::text				_641_tm,
_op204_tm::text				_op204_tm,
_204_staging::text			_204_staging,
_op205_tm::text				_op205_tm,
_205_id::text				_205_id,
_205_staging::text 	_205_staging,
"_70_tm"::text    _70_tm,
''''::text			"209_last",
"205_last"::text 	"205_last",
"80_last"::text   "80_last",
"80_id"::text     "80_id",
"80_id_staging"::text  "80_id_staging",
opcode201::text   opcode201,
opcode206::text   opcode206,
opcode55::text    opcode55,
"_90_tm"::text    "_90_tm",
latest_33::text   latest_33,
tm_92::text       tm_92,
op_59::text       op_59,
op_56::text       op_56,
why_code_60::text 				why_code_60,
"60_why_code_last"::text  		"60_why_code_last",
why_note_last_60::text    		why_note_last_60,
"_70_why_code"::text			_70_why_code,
"70_why_code_last"::text		"70_why_code_last",
"_70_why_notes_last"::text		_70_why_notes_last,
why_code_33::text			why_code_33,
why_code_92::text			why_code_92,
why_notes_92::text			why_notes_92,
bagseal_no::text			bagseal_no,
attempt60::text				attempt60,
attempt70::text				attempt70,
"_70_tm_first"::text 		_70_tm_first,
"_70_why_code_first"::text	_70_why_code_first,
return_awb::text			return_awb,
etd_returned::text			etd_returned,
latest_op_5::text			latest_op_5,
latest_scan_time_5::text	latest_scan_time_5,
latest_scan_op_5::text		latest_scan_op_5,
latest_hub_5::text			latest_hub_5,
latest_staging_5::text		latest_staging_5,
"80_tm_5"::text				"80_tm_5",
''''::text					flag_pod,
consignee::text 			consignee,
item_name::text				item_name,
item_category::text			item_category,
flag_cod::text				flag_cod,
''''::text					cod_amount,
merchant_weight::text       merchant_weight,
actual_weight::text         actual_weight,
fee_amt_ongkir::text        fee_amt_ongkir,
parcel_total_weight::text   parcel_total_weight,
harga_barang::text          harga_barang,
insurance::text             insurance,
delivery_due_edited::text   delivery_due_edited,
delivery_sla_edited::text   delivery_sla_edited,
"3PL"::text				"3PL",
''''::text				"Live Resi",
start_return::text 		"Start Return",
done_return::text  		"Done Return",
autoclaim_due::text		autoclaim_due,
channel::text 			channel
from b2b.b2b_retention a 
left join b2b.tt_opcode_description b on a.fop_code_internal = b.opcode and a.fop_code_internal not in (''80'',''90'') 
			AND COALESCE( a."70_why_code_last", a."60_why_code_last" ) = b.staywhycode
left join b2b.tm_bar_opt_code c on a.fop_code_internal = c.op_code
left join b2b.tb_mv_rmds_unitzone_v2 d on a.origin_uz = d.receive_unitarea and d.flag_pud=''0''
left join b2b.tb_mv_rmds_unitzone_v2 e on a.destination_uz =e.receive_unitarea and e.flag_pud=''0''
where 1=1
                    ' || startDate_query 
					  || endDate_query ||'::date + interval ''1 day''
					' || awb_source_query
                      || latest_opcode_internal_query
                      || latest_opcode_external_query || '
                 ';
    /**
     * Building Final Query
     */
    final_query := awb_source || base_query || offset_query;

    RAISE NOTICE 'final_query : % ', final_query;
	--RAISE NOTICE 'v_awb : % ', v_awb; 

    if p_download = 1 then

        select id
        into v_report_id
        from analysis_services.tm_report_name
        where report_name = p_report_name;

        call analysis_services.proc_aware_ins_download_request(v_report_id, p_nik, final_query, p_distribution_type);

    else
        RETURN QUERY EXECUTE final_query;
    end if;

--     RETURN QUERY EXECUTE final_query;
END ;
$function$
;
