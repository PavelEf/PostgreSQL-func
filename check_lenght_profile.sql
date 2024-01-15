CREATE OR REPLACE FUNCTION zz_efr.check_lenght_profile(doc_date_param date, doc_id_param integer, OUT v_delta integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
    result int;
begin
with t as (
	select s1.st_id as st_id1, s2.st_id as st_id2, s1.nord 
    from mm.m2_sched s1 
    join mm.m2_sched s2 on s2.doc_date = s1.doc_date and s2.doc_id = s1.doc_id and s2.nord = s1.nord + 1 
    where s1.doc_date = doc_date_param and s1.doc_id = doc_id_param 
 ),  
 t1 as ( 
    select t.st_id1, t.st_id2, a.id 
    from t 
    join nsi.path_abinfo a on a.stida = t.st_id1 and a.stidb = t.st_id2 
 ), 
 t2 as ( 
    select t1.st_id1, t1.st_id2, t1.id, ab.id_way, ab.stida, ab.stidb 
    from t1 
    join nsi.path_ab ab on ab.idfind = t1.id 
 ), 
 t3 as ( 
  	select t2.st_id1, t2.st_id2, t2.id, t2.id_way, t2.stida, t2.stidb, sw1.x as x1, sw2.x as x2, sw1.way_id 
    from t2 
    join nsi.st_way sw1 on sw1.way_id = t2.id_way and sw1.st_id = t2.stida 
    join nsi.st_way sw2 on sw2.way_id = t2.id_way and sw2.st_id = t2.stidb 
 ), 
 t4 as ( -- профиль 
  	select t3.st_id1, t3.st_id2, t3.id, t3.id_way, t3.stida, t3.stidb,  
  	t3.x1, t3.x2, wp.ord_num, wp.grad, wp.uklon_pure,  
  	case when wp.x < t3.x1 then t3.x1 else wp.x end as x,  
  	case 
   	when wp.x < t3.x1 then wp.len - (t3.x1 - wp.x) 
   	when wp.x + wp.len > t3.x2 then t3.x2 - wp.x 
   	else wp.len 
    end as len, 
    wp.len as len_orig 
    from t3 
    join nsi.way_profile as wp on wp.way_id = t3.id_way 
    where wp.x between t3.x1 and t3.x2 or (wp.x + wp.len > t3.x1 and wp.x <= t3.x2) 
 ), 
 t5 as ( -- длина и количество элементов для каждой дороги 
    select t4.id_way, 
    count(t4.id_way) as count_elements, 
    sum(t4.len) as total_len
    from t4
    group by t4.id_way
 ), 
 t6 as ( 
  	select t4.st_id1, t4.st_id2, t4.id, t4.id_way, t4.stida, t4.stidb, t4.x1, t4.x2,  
  	t4.ord_num, t4.grad, t4.uklon_pure, t4.x, t4.len, t4.len_orig,  
  	t5.count_elements, t5.total_len 
  	from t4 
  	join t5 on t5.id_way = t4.id_way 
 ), 
 check_res as (
  	select case
    when exists (
    	select id_way, len from t6
        except
        select id_way, total_len from t5
        union
        select id_way, total_len from t5
        except
        select id_way, len from t6
	) then (select sum(t6.len) from t6) - (select sum(t3.x2 - t3.x1) from t3 limit 1)
    else 0
    end as check_result
 )
    select check_result into v_delta 
    from check_res; 
end; 
$function$
;
