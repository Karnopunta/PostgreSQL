-- DROP FUNCTION analysis_services.f_get_pickup_sla_v7(json);

CREATE OR REPLACE FUNCTION analysis_services.f_get_pickup_sla_v7(p_json json)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
    p_bulk_shipper     character varying := p_json ->> 'p_bulk_shipper';
    p_54_tm            timestamp without time zone := p_json ->> 'p_54_tm';
    p_54_dt            date := p_json ->> 'p_54_dt';
    p_expect_finish_tm timestamp without time zone := p_json ->> 'p_expect_finish_tm';
    p_expect_finish_dt date := p_json ->> 'p_expect_finish_dt';
    p_order_tm         timestamp without time zone := p_json ->> 'p_order_tm';
    p_order_dt         date := p_json ->> 'p_order_dt';
    p_customer_code    character varying := p_json ->> 'p_customer_code';
    p_service_code     character varying := p_json ->> 'p_service_code';
    p_60_why_code      character varying := p_json ->> 'p_60_why_code';
    p_60_tm            timestamp without time zone := p_json ->> 'p_60_tm';
    p_44_tm            timestamp without time zone := p_json ->> 'p_44_tm';
    p_type             character varying := p_json ->> 'p_type';
    p_seller_opt             character varying := p_json ->> 'p_seller_opt';
    v_sla              text;
    v_dow              integer;
    p_client_order_no character varying := p_json ->> 'p_client_order_no';
BEGIN

    if p_customer_code = '3174000014' and p_service_code in ('REG', 'ND') -- byU reg dan ND
    then
        v_dow := extract(dow from p_order_tm);
        if v_dow in (0, 6)-- sabtu minggu
        then
            if v_dow = 0 -- minggu
            then -- modified 2019-09-29 diubah dari '17:30:00'::interval menjadi '19:00:00'::interval; request by Jodie approval ansel mas Andre
                p_expect_finish_tm := p_order_dt + interval '1 day' + '19:00:00'::interval;
--                 p_expect_finish_dt := p_expect_finish_tm::date;
            elseif v_dow = 6 and extract(hour from p_order_tm) >= 12 then -- sabtu diatas jam 12
                p_expect_finish_tm := p_order_dt + interval '2 day' + '19:00:00'::interval;
--                 p_expect_finish_dt := p_expect_finish_tm::date;
            else -- sabtu sebelum jam 12
                p_expect_finish_tm := p_order_dt + '19:00:00'::interval;
--                 p_expect_finish_dt := p_expect_finish_tm::date;
            end if;
        else
            if p_order_tm::time >= '16:30:00'::time then --weekday setelah 16.30
                p_expect_finish_tm := p_order_dt + interval '1 day' + '19:00:00'::interval;
--                 p_expect_finish_dt := p_expect_finish_tm::date;
            else -- weekday sebelum 16.30
                p_expect_finish_tm := p_order_dt + '19:00:00'::interval;
--                 p_expect_finish_dt := p_expect_finish_tm::date;
            end if;
        end if;
        p_expect_finish_dt := p_expect_finish_tm::date;
    elseif p_customer_code in ('3171000008', '3671000004')
    then
        if p_order_tm::time >= '15:00:00'::time then --weekday setelah 12.00, diubah jd 15:00 20211021 no story 106601
            p_expect_finish_tm := p_order_dt + interval '1 day' + '23:59:59'::interval;
--                 p_expect_finish_dt := p_expect_finish_tm::date;
        else -- weekday sebelum 16.30
            p_expect_finish_tm := p_order_dt + '23:59:59'::interval;
--                 p_expect_finish_dt := p_expect_finish_tm::date;
        end if;

        p_expect_finish_dt := p_expect_finish_tm::date;
    elseif p_customer_code in ('1000009') and p_service_code = 'CSD' -- toko a
    then
        p_expect_finish_tm := p_order_tm + '00:45:00'::interval;
        p_expect_finish_dt := p_expect_finish_tm::date;
    end if;

    -- hitung SLA
    if p_type = '1' then -- sla with exception
        if p_60_why_code in ('17', '3', '703', '25', '701', '702') and p_54_tm is not null then
            p_54_tm := p_60_tm;
            p_54_dt := p_60_tm::date;
        end if;
    end if;

    if (p_bulk_shipper IS NOT NULL) THEN
        v_sla := 'OK'::text;
    elseif 
        (p_customer_code in ('1000001','100002','100003','10000009','1000014','1000004')) THEN
        v_sla := 'OK'::text;
    elseif -- dropoff 20230901
        (p_seller_opt = 'DROPOFF') THEN
        v_sla := 'OK'::text;
    elseif -- orchestra MM SLA nya OK 20230529
        (p_customer_code = '3174000041' and p_44_tm is not null) THEN
        v_sla := 'OK'::text;
    elseif -- awb reverse sla OK 20240620
-- right(p_client_order_no,2) = '-R' THEN
            right(p_client_order_no,2) = '–R' THEN
        v_sla := 'OK'::text;
    elseif
        (p_54_tm IS NOT NULL) THEN
        if ((p_54_tm > p_expect_finish_tm) AND (p_54_dt = p_expect_finish_dt))
        THEN
            v_sla = 'Missed by Hour'::text;
        elseif
            (p_54_tm > p_expect_finish_tm) THEN
            v_sla = 'Missed'::text;
        ELSE
            v_sla = 'OK'::text;
        END if;
    ELSE
        /*
         semua awb tanpa opcode 54 = In Progress 20230329 story: 214487
         */
        v_sla = 'In Progress'::text;

    END if;

    return v_sla;
END ;
$function$
;
