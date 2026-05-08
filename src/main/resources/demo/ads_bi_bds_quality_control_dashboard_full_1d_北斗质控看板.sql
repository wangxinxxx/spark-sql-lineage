--***************************************************************************************************
-- ** 文件名称：hdp_zhuanzhuan_ads_global.ads_bi_bds_quality_control_dashboard_full_1d.sql
-- ** 功能描述： 北斗质控表


-- ** 脚本开发：江新源
-- ** 脚本部署：江新源
-- ** 创建日期：2025-07-10
-- ** 更新日期：2025-07-10
--  需求链接  https://www.tapd.cn/tapd_fe/20848741/story/detail/1120848741001648381

--***************************************************************************************************





-- drop table if exists hdp_zhuanzhuan_ads_global.ads_bi_bds_quality_control_dashboard_inc_1d ;
-- CREATE TABLE hdp_zhuanzhuan_ads_global.ads_bi_bds_quality_control_dashboard_inc_1d ( 
--      stat_date	                                      string	comment'统计日期'
--     ,merchant_id	                                  bigint	comment'商户id'
--     ,merchant_name	                                  string	comment'商户名称'
--     ,supplier_region_name 	                          string	comment'供应商所在大区'
--     ,supplier_site_name 	                          string	comment'供应商站点名称'
--     ,central_warehouse_site_name  	                  string	comment'中心仓站点名称'
--     ,b2c_business_mode	                              string	comment'B2C业务模式'
--     ,cate_name	                                      string	comment'品类名称'
--     ,brand_id	                                      bigint	comment'品牌id'
--     ,brand_name	                                      string	comment'品牌名称'
--     ,model_id	                                      bigint	comment'机型id'
--     ,model_name	                                      string	comment'机型名称'
--     ,system	                                          string	comment'系统名称'
--     ,cargo_tray_classify 	                          string	comment'自定义货盘分类'
--     ,spec_appearance_quality	                      string	comment'外观成色'
--     ,spec_function_quality	                          string	comment'功能成色'
--     ,actual_post_type_name	                          string	comment'实际后验类型'
--     ,sign_order_num	                                  int	    comment'签收订单量'
--     ,appearance_issue_order_num	                      int	    comment'综合外观问题订单量'
--     ,function_issue_order_num	                      int	    comment'功能类质量问题订单量'
--     ,four_day_appearance_issue_order_num	          int	    comment'4天内综合外观问题订单量'
--     ,four_day_function_issue_order_num 	              int	    comment'4天内功能类质量问题订单量'
--     ,nps_survey_order_num	                          int	    comment'nps问卷订单量'
--     ,nps_survey_promoter_order_num	                  int	    comment'nps问卷推荐订单量'
--     ,nps_survey_neutral_order_num	                  int	    comment'nps问卷中立订单量'
--     ,nps_survey_detractor_order_num	                  int	    comment'nps问卷贬损订单量'
--     ,qc_num	                                          int	    comment'质检量'
--     ,motherboard_image_review_num	                  int	    comment'主板图审核量'
--     ,motherboard_image_review_reject_num	          int	    comment'主板图审核不通过量'
--     ,shelf_on_num	                                  int	    comment'上架量'
--     ,shelf_off_num	                                  int	    comment'下架量'
--     ,re_shelf_on_num	                              int	    comment'再上架量'
--     ,defect_image_num	                              int	    comment'瑕疵图数量'
--     ,appearance_defect_item_num	                      int	    comment'外观瑕疵项勾选量'
--     ,post_inspection_num	                          int	    comment'后验质检量'
--     ,post_intercept_num	                              int	    comment'后验拦截量'
--     ,post_intercept_success_num	                      int	    comment'后验拦截干预成功量'
--     ,post_appearance_intercept_num	                  int	    comment'后验外观拦截量'
--     ,post_repair_intercept_num	                      int	    comment'后验拆修拦截量'
--     ,post_function_intercept_num	                  int	    comment'后验功能拦截量'
--     ,post_other_intercept_num  	                      int	    comment'后验其他拦截量'
--     ,actual_disassembly_num	                          int	    comment'实际拆机量'
--     ,disassembly_found_num 	                          int	    comment'拆中量'
--     ,disassembly_damage_num  	                      int	    comment'拆损量'
--    )  
--    comment'北斗质控聚合表'
--    partitioned BY (dt string COMMENT 'yyyy-MM-dd')
--    STORED AS PARQUET;


-- 数据源



-- hdp_zhuanzhuan_dw_global.dw_perform_qc_normal_qc_detail_full_1d 
-- hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d 
-- hdp_zhuanzhuan_dim_global.dim_sup_suppiler_full_1d_0p   
-- hdp_zhuanzhuan_dm_global.dm_perform_qc_feedback_b2c_order_detail_full_1d 
-- hdp_zhuanzhuan_dim_global.dim_business_customize_dict_full_1d_0p 
-- hdp_ubu_zhuanzhuan_dim_b2c.dim_info_b2c_prod_basic_info_full_1d 
-- hdp_zhuanzhuan_dm_global.dm_perform_kf_work_order_detail_full_1d
-- hdp_zhuanzhuan_dw_global.dw_perform_kf_ticket_compensate_detail_full_1d
-- hdp_zhuanzhuan_dw_global.dw_perform_afs_order_basic_detail_full_1d
-- hdp_zhuanzhuan_dw_global.dw_perform_afs_evidence_detail_full_1d
-- hdp_zhuanzhuan_dw_global.dw_user_interact_nps_investigate_detail_full_1d 
-- hdp_zhuanzhuan_dw_global.dw_trade_order_company_all_detail_full_1d 
-- hdp_zhuanzhuan_dw_global.dw_perform_qc_after_sell_diff_detail_full_1d
-- hdp_zhuanzhuan_dw_global.dw_perform_handover_order_full_1d
-- hdp_zhuanzhuan_dw_global.dw_perform_oms_outbound_order_detail_full_1d 
-- hdp_zhuanzhuan_dw_global.dw_perform_qc_substation_audit_detail_full_1d 
-- hdp_zhuanzhuan_dim_global.dim_perform_qc_report_split_detail_full_1d 
-- hdp_ubu_zhuanzhuan_dw_b2c.dw_info_prod_opt_detail_inc_1d
-- hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_qc_photo_version_full_1d
-- hdp_zhuanzhuan_dw_global.dw_perform_kf_ticket_wide_full_1d  
-- hdp_zhuanzhuan_dw_global.dw_perform_kf_artificial_call_detail_full_1d
--  hdp_zhuanzhuan_rawdb_global.raw_mysql_tdb_qc_digitization_qc_spot_check_pre_allocate_list_flow_full_1d
--   hdp_zhuanzhuan_dw_global.dw_perform_qc_qa_list_strategy_dtl_full_1d
--  hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_ai_xray_photo_recognition_full_1d
--  hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_brain_qc_condition_rule_full_1d
--  hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_brain_qc_condition_rule_detail_full_1d
-- hdp_zhuanzhuan_dw_global.dw_perform_qc_damaged_detail_full_1d
-- hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_qc_after_sell_req_info_full_1d
-- hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_brain_qc_item_extend_full_1d
-- hdp_zhuanzhuan_dim_global.dim_perform_kf_business_line_full_1d_0p
-- hdp_zhuanzhuan_dm_global.dm_perform_qc_after_sell_diff_ot_detail_all_inc_1d

-- 剔除掉
-- hdp_zhuanzhuan_dim_global.dim_sup_suppiler_full_1d_0p  
--  hdp_ubu_zhuanzhuan_dim_b2c.dim_scm_supplier_full_1d_0p 



drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_001  ;
create table if not exists hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_001  as
--  获取商家对应的已验机站点名称
with  b2c_qc_site_name as (
select 
     t2.order_id
    ,t1.store_name     as supplier_site_name
    ,row_number() over(  distribute by t2.order_id sort by ( unix_timestamp(t2.pay_time) - unix_timestamp(t1.qc_time)) asc ) AS rn
from 
    hdp_zhuanzhuan_dw_global.dw_perform_qc_normal_qc_detail_full_1d  t1   --  商户质检明细数据
join 
    hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d  t2    --  b2c订单
on   t1.qc_code =    cast( t2.qc_code  as bigint )     and    t2.cate_first_id  = 101     and  nvl(t2.pay_time,'') >= '2025-02-01'   and  t2.dt='2026-04-15'
where  t1.dt='2026-04-15'     and  unix_timestamp(t2.pay_time) - unix_timestamp(t1.qc_time) > 0    -- 支付时间 大于  质检时间  
   having rn = 1
)


select 
     t1.pay_time                                                as pay_time
    ,t1.order_id                                                as order_id
    ,t1.seller_id                                               as merchant_id 
    ,t2.supplier_name                                           as merchant_name
    ,t2.department_name                                         as supplier_region_name
    ,t3.supplier_site_name                                      as supplier_site_name
    ,t4.central_warehouse_site_name                             as central_warehouse_site_name
    ,t1.b2c_business_mode                                       as b2c_business_mode
    ,t1.cate_first_name                                         as cate_name
    ,cast( t1.brand_id  as bigint )                             as brand_id
    ,t1.brand_name                                              as brand_name
    ,cast( t1.model_id  as bigint )                             as model_id
    ,t1.model_name                                              as model_name
    ,nvl( get_json_object(t5.item_desc, '$[2]'),'')             as system
    ,nvl( get_json_object(t5.item_desc, '$[3]'),'')             as cargo_tray_classify
    ,t6.spec_appearance_quality                                 as spec_appearance_quality
    ,t6.spec_function_quality                                   as spec_function_quality
    ,''                                                         as actual_post_type_name
    ,if(  length(t1.user_signed_time) > 0 ,1,0  )               as is_sign_order
    ,t1.user_signed_time                                        as  user_signed_time
from  
    hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d     t1   --  b2c订单表
left join  
   hdp_ubu_zhuanzhuan_dim_b2c.dim_scm_supplier_full_1d_0p                   t2 
ON   t1.seller_id =  t2.uid AND   t2.uid <> 0
left join 
      b2c_qc_site_name         t3 
on  t1.order_id = t3.order_id   and t3.rn = 1 
left join 
(
    select 
         order_id
        ,to_store_name                   as   central_warehouse_site_name 
    from 
          hdp_zhuanzhuan_dm_global.dm_perform_qc_feedback_b2c_order_detail_full_1d 
    where  dt='2026-04-15'
        group by 1,2  
) t4  
on t1.order_id = t4.order_id
left join
      hdp_zhuanzhuan_dim_global.dim_business_customize_dict_full_1d_0p    t5 
 on t1.model_id  = t5.item_key  and   t5.data_dict_code =  'raw_manual_cargo_tray_classify_full_1d_0p' 

left join 
     hdp_ubu_zhuanzhuan_dim_b2c.dim_info_b2c_prod_basic_info_full_1d    t6 
on t1.info_id = t6.info_id and  nvl( t1.yp_code,'') = nvl(t6.yp_code,'') and     t6.dt =   '2026-04-15' 

where  t1.dt = '2026-04-15'  
and   to_date(t1.pay_time) >= '2025-02-01'     --  限制时间为 2025.02.01  
and   t1.cate_first_id   = 101                  --  限制品类为 手机 
and   t1.b2c_business_mode   in ('CZC',  'B2C商户-已验机2.0', 'B2C商户-已验机plus' ,'采货侠B1货源','采货侠资源机','B2C商户-入仓质检' ) ;




drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_002  ;
create table if not exists hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_002  as
select 
    t1.order_id 
   ,max( if(   t2.order_id  is not null   or t3.order_id  is not null  or t4.order_id  is not null ,1,0 ))        as is_appearance_issue
   ,max( if(  t5.order_id  is not null  ,1,0   ))                                                                 as is_function_issue
   ,max( if(  ( t2.order_id  is not null  or t3.order_id  is not null or t4.order_id  is not null )  and   unix_timestamp( LEAST( t2.creat_work_order_time, t3.compensate_success_time, t4.afs_order_create_time)) 
       -unix_timestamp( t1.user_signed_time)  <= 4*60*60,1,0  ) )                                                                                 as is_4h_appearance_issue
   ,max( if(  t5.order_id  is not null    and unix_timestamp(t5.afs_order_create_time) -unix_timestamp( t1.user_signed_time)  <= 4*60*60,1,0  ) ) as is_4h_function_issue
  
from 
      hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_001     t1
left join 
( 
    select 
         t1.order_id
        ,t1.create_work_order_time    as  creat_work_order_time   
    from 
         hdp_zhuanzhuan_dw_global.dw_perform_kf_ticket_wide_full_1d   t1
    left join 
          hdp_zhuanzhuan_dim_global.dim_perform_kf_business_line_full_1d_0p t2 
    on  t1.work_order_id = t2.relation_id  and t2.type =  2   -- 工单         
    where   t1.dt = '2026-04-15' 
       and  t2.first_business_line_name   = 'B2C'
       and  t1.work_order_classify  in   ('内投','舆情')  
       and  ( SUBSTRING_INDEX(  t1.work_order_problem_attribution , '/', 4)   in ( 'B2C业务/质检-两侧报告不一致/与C2B验机不一致/外观成色',  'B2C业务/质检-两侧报告不一致/与寄卖验机不一致/外观成色' ) 
          or ( SUBSTRING_INDEX( t1.work_order_problem_attribution, '/', 3) = 'B2C业务/质检-同侧验机不一致/外观成色'  and    nvl(split(t1.work_order_problem_attribution,'/')[3],'')    NOT RLIKE '^报告描述一致'  ) )   
)  t2 
on t1.order_id = cast( t2.order_id  as bigint )   

left join 
(
     select 
         order_id
        ,compensate_success_time
     from
           hdp_zhuanzhuan_dw_global.dw_perform_kf_ticket_compensate_detail_full_1d
    where   dt = '2026-04-15' 
        and  compensate_type_id  = 2                  -- 2 赔付类型 为打款
        and  compensation_result   =  '打款成功'      --  打款成功
        and  compensate_success_time  <> ''           
        and  first_compensation_reason in ( '自营站点责任 （全国分站/验机中心）','已验机站点责任 （1.0、2.0、plus、采货侠）' ) 
        and  second_compensation_reason = '验机失误'
        and  four_compensation_reason =  '外观成色'   
) t3 
on t1.order_id = cast(  t3.order_id  as bigint )

left join 
(
    select 
         t1.order_id
        ,t1.create_time                  as afs_order_create_time
    from 
         hdp_zhuanzhuan_dw_global.dw_perform_afs_order_basic_detail_full_1d  t1 
     join 
         hdp_zhuanzhuan_dw_global.dw_perform_afs_evidence_detail_full_1d   t2 
     on  t1.afs_order_id  =  t2.afs_order_id    and  t2.dt = '2026-04-15'    
    where t1.dt = '2026-04-15'     and  t2.first_optional_afs_reason  like  "外观" 
) t4 
on t1.order_id = t4.order_id  

left  join 
(
     select 
           t1.order_id
          ,t1.create_time               as afs_order_create_time
     from 
          hdp_zhuanzhuan_dw_global.dw_perform_afs_order_basic_detail_full_1d   t1 
     join 
          hdp_zhuanzhuan_dw_global.dw_perform_afs_evidence_detail_full_1d   t2 
     on t1.afs_order_id  =  t2.afs_order_id  and  t2.dt = '2026-04-15'
     where  t1.dt = '2026-04-15'  
        and  t2.first_optional_afs_reason  not like "外观" 
        and ( t2.first_optional_afs_reason  regexp '功能|显示|维修|连接|不符' 
             or t2.first_optional_afs_reason  regexp  '机器质量有问题' )     
) t5
on t1.order_id = t5.order_id  
where   t1.is_sign_order = 1    --  代表已签收  就是用户签收时间不为空 
    group  by  t1.order_id
;




-- 以支付时间作为统计日期
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_003  ;
create table if not exists hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_003  as
select 
      to_date(t1.pay_time)              as   stat_date                           
     ,t1.merchant_id	                              
     ,t1.merchant_name	                          
     ,t1.supplier_region_name 	                  
     ,t1.supplier_site_name 	                      
     ,t1.central_warehouse_site_name  	          
     ,t1.b2c_business_mode	                      
     ,t1.cate_name	                              
     ,t1.brand_id	                              
     ,t1.brand_name	                              
     ,t1.model_id	                              
     ,t1.model_name	                              
     ,t1.system	                                  
     ,t1.cargo_tray_classify 	                  
     ,t1.spec_appearance_quality	                  
     ,t1.spec_function_quality	                  
     ,t1.actual_post_type_name	   
     ,count(  distinct if(  t1.is_sign_order  = 1  , t1.order_id,null ) )                as   sign_order_num	                          
     ,count(  distinct if(  t2.is_appearance_issue  = 1  , t1.order_id,null ) )          as   appearance_issue_order_num	              
     ,count(  distinct if(  t2.is_function_issue  = 1  , t1.order_id,null ) )            as   function_issue_order_num	              
     ,count(  distinct if(  t2.is_4h_appearance_issue  = 1  , t1.order_id,null ) )       as   four_day_appearance_issue_order_num	      
     ,count(  distinct if(  t2.is_4h_function_issue  = 1  , t1.order_id,null ) )         as   four_day_function_issue_order_num 	          
from 
    hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_001  t1 
left join 
    hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_002   t2 
on  t1.order_id = t2.order_id    
where  t1.is_sign_order = 1  
group by   
     to_date(t1.pay_time)                                
    ,t1.merchant_id	                              
    ,t1.merchant_name	                          
    ,t1.supplier_region_name 	                  
    ,t1.supplier_site_name 	                      
    ,t1.central_warehouse_site_name  	          
    ,t1.b2c_business_mode	                      
    ,t1.cate_name	                              
    ,t1.brand_id	                              
    ,t1.brand_name	                              
    ,t1.model_id	                              
    ,t1.model_name	                              
    ,t1.system	                                  
    ,t1.cargo_tray_classify 	                  
    ,t1.spec_appearance_quality	                  
    ,t1.spec_function_quality	                  
    ,t1.actual_post_type_name	;                  
   





-- nps 模块
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_004  ;
create table if not exists hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_004  as
with  b2c_nps_order  as (
    select 
         create_time
        ,order_id
        ,score_level
    from 
          hdp_zhuanzhuan_dw_global.dw_user_interact_nps_investigate_detail_full_1d 
    where  dt = '2026-04-15'
            AND is_delete = 0                                     -- 筛选有效问卷
            AND survey_type = 'nps'                               -- 筛选NPS类型调查
            AND business_type IN ('b2c', 'zljb2c', 'chuangxin')   -- 筛选业务类型
            AND order_id IS NOT NULL                              -- 确保有订单ID
             -- 排除家电相关调研
            AND title NOT IN ('转转用户【交通工具\家具家电购买体验】调研', '【转转严选用户】手机购买体验调研')
            and to_date(create_time)  >= '2025-02-01'
    group by 1,2,3        
)

select 
     to_date(t2.create_time)               as   stat_date                           
     ,t1.merchant_id	                              
     ,t1.merchant_name	                          
     ,t1.supplier_region_name 	                  
     ,t1.supplier_site_name 	                      
     ,t1.central_warehouse_site_name  	          
     ,t1.b2c_business_mode	                      
     ,t1.cate_name	                              
     ,t1.brand_id	                              
     ,t1.brand_name	                              
     ,t1.model_id	                              
     ,t1.model_name	                              
     ,t1.system	                                  
     ,t1.cargo_tray_classify 	                  
     ,t1.spec_appearance_quality	                  
     ,t1.spec_function_quality	                  
     ,t1.actual_post_type_name
     -- 评价等级 0:贬低 1:中立 2:推荐
     ,count(  distinct t2.order_id )                                       as  nps_survey_order_num	        
     ,count(  distinct  if( t2.score_level  = 2  , t2.order_id,null ) )    as  nps_survey_promoter_order_num	
     ,count(  distinct  if( t2.score_level  = 1  , t2.order_id,null ) )    as  nps_survey_neutral_order_num	
     ,count(  distinct  if( t2.score_level  = 0  , t2.order_id,null ) )    as  nps_survey_detractor_order_num		   
from 
      hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_001     t1 
join      
     b2c_nps_order    t2 
on   t1.order_id = t2.order_id 
where  to_date(t2.create_time)  >= '2025-02-01'  
group by   
     to_date(t2.create_time)                                
    ,t1.merchant_id	                              
    ,t1.merchant_name	                          
    ,t1.supplier_region_name 	                  
    ,t1.supplier_site_name 	                      
    ,t1.central_warehouse_site_name  	          
    ,t1.b2c_business_mode	                      
    ,t1.cate_name	                              
    ,t1.brand_id	                              
    ,t1.brand_name	                              
    ,t1.model_id	                              
    ,t1.model_name	                              
    ,t1.system	                                  
    ,t1.cargo_tray_classify 	                  
    ,t1.spec_appearance_quality	                  
    ,t1.spec_function_quality	                  
    ,t1.actual_post_type_name	;  






-- 优化版本：兼容Spark SQL，保持逻辑不变，提升性能和可读性
DROP TABLE IF EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005;
CREATE TABLE IF NOT EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005 AS
-- 1. 基础商品信息预处理（添加时间戳预计算）
WITH base_product_info AS (
    SELECT 
        t1.info_id ,
        cast(t1.qc_code AS BIGINT) AS qc_code,
        t1.online_time,
        UNIX_TIMESTAMP(t1.online_time) AS online_timestamp_unix,
        
        -- 预计算时间戳，避免重复转换
        FROM_UNIXTIME(cast(t1.create_timestamp/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss')    AS create_time,
        cast(t1.create_timestamp/1000 AS BIGINT)                                          AS create_timestamp_unix,
        t1.uid,
        t1.b2c_business_mode,
        t1.cate_first_name,
        cast( t1.brand_id AS BIGINT) AS brand_id,
        t1.brand_name,
        cast( t1.model_id AS BIGINT) AS model_id,
        t1.model_name,
        t1.spec_appearance_quality,
        t1.spec_function_quality,
        max( if(t2.order_id is not null,1,0))    as is_onsale
    FROM 
         hdp_ubu_zhuanzhuan_dim_b2c.dim_info_b2c_prod_basic_info_full_1d t1      
    left join  
        hdp_zhuanzhuan_dw_global.dw_trade_order_company_all_detail_full_1d  t2   
    on  t1.info_id = t2.info_id  and  nvl( t1.yp_code,'') = nvl(t2.yp_code,'')  and   t2.dt = '2026-04-15'
    WHERE   t1.dt = '2026-04-15'
         AND t1.cate_first_id = 101  
         AND t1.b2c_business_mode in ( 'CZC'  ,'B2C商户-已验机2.0' ,'B2C商户-已验机plus' ,'B2C商户-入仓质检','采货侠B1货源','采货侠资源机') 
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15    

),



-- 2. 质检详情预处理（添加时间戳预计算）
qc_detail_processed AS (
     SELECT 
        qc_code,
        store_name,
        qc_time,
        UNIX_TIMESTAMP(qc_time)      AS qc_timestamp_unix
    FROM hdp_zhuanzhuan_dw_global.dw_perform_qc_normal_qc_detail_full_1d  
    WHERE dt = '2026-04-15'
),





-- 3. 供应商质检站点（使用子查询替代QUALIFY）
supplier_qc_site_ranked AS (
    SELECT 
        p.info_id,
        q.store_name    AS supplier_site_name,
        q.qc_time       as qc_time,
        ROW_NUMBER() OVER(
            PARTITION BY p.info_id 
            ORDER BY (  p.online_timestamp_unix   >  q.qc_timestamp_unix ) ASC
        ) AS rn
    FROM 
         base_product_info p
    INNER JOIN 
         qc_detail_processed q ON p.qc_code = q.qc_code
    WHERE  p.online_timestamp_unix   >   q.qc_timestamp_unix  --  先质检 再上架 
    having rn = 1
),



-- 4. 售后差异详情预处理
after_sell_diff_base AS (
    SELECT  
        t1.qc_code,
        t3.store_name,
        t1.aft_sel_operate_time   AS finish_time,
        UNIX_TIMESTAMP(t1.aft_sel_operate_time) AS finish_timestamp_unix
    FROM 
         hdp_zhuanzhuan_dw_global.dw_perform_qc_after_sell_diff_detail_full_1d t1 
    LEFT JOIN 
         hdp_zhuanzhuan_dw_global.dw_perform_handover_order_full_1d t2 
   ON t1.qc_code = t2.qc_code 
      AND t1.handover_order_id = t2.handover_order_id    
        AND t2.dt = '2026-04-15'
    LEFT JOIN hdp_zhuanzhuan_dw_global.dw_perform_oms_outbound_order_detail_full_1d t3 
        ON t2.handover_no = t3.business_order_id  
        AND t3.dt = '2026-04-15'
    WHERE t1.dt = '2026-04-15'    
),

-- 5. 中心仓站点（使用子查询替代QUALIFY）
central_warehouse_site_ranked AS (
    SELECT
        p.info_id,
        a.store_name    AS central_warehouse_site_name,
        ROW_NUMBER() OVER(
            PARTITION BY p.info_id 
            ORDER BY (a.finish_timestamp_unix - p.online_timestamp_unix) ASC
        ) AS rn
    FROM base_product_info p  
    INNER JOIN after_sell_diff_base a ON p.qc_code = a.qc_code   
    WHERE a.finish_timestamp_unix > p.online_timestamp_unix  -- 先上架才走后验
       having rn = 1
)



-- 6. 主查询（优化JOIN顺序和添加注释）
SELECT  
    p.info_id,
    p.qc_code,
    p.online_time,
    p.create_time,
    p.create_timestamp_unix,
    p.online_timestamp_unix,

    p.uid AS merchant_id,
    -- 商户信息关联
    s.supplier_name AS merchant_name,
    s.department_name AS supplier_region_name,
    -- 站点信息关联  
    sq.supplier_site_name,
    sq.qc_time,
    cw.central_warehouse_site_name,
    -- 业务分类信息
    p.b2c_business_mode,
    p.cate_first_name    AS cate_name,
    p.brand_id,
    p.brand_name,
    p.model_id,
    p.model_name,
    -- 系统和货盘分类信息（优化JSON解析）
    COALESCE(GET_JSON_OBJECT(d.item_desc, '$[2]'), '') AS system,
    COALESCE(GET_JSON_OBJECT(d.item_desc, '$[3]'), '') AS cargo_tray_classify,
    -- 质量规格信息
    p.spec_appearance_quality,
    p.spec_function_quality,
    '' AS actual_post_type_name,
    p.is_onsale
FROM base_product_info p
-- 使用LEFT JOIN保证数据完整性，按重要性排序
LEFT JOIN hdp_ubu_zhuanzhuan_dim_b2c.dim_scm_supplier_full_1d_0p  s 
    ON p.uid = s.uid AND s.uid <> 0  
LEFT JOIN supplier_qc_site_ranked  sq
    ON p.info_id = sq.info_id
LEFT JOIN central_warehouse_site_ranked cw    
    ON p.info_id = cw.info_id
LEFT JOIN hdp_zhuanzhuan_dim_global.dim_business_customize_dict_full_1d_0p d 
    ON p.model_id = CAST(d.item_key AS BIGINT)  
    AND d.data_dict_code = 'raw_manual_cargo_tray_classify_full_1d_0p'
where  P.uid  !='38568694471445'    --  剔除测试账号
      and   nvl(P.qc_code,0)  <> 0   ;




DROP TABLE IF EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_007;
CREATE TABLE IF NOT EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_007 AS
select 
    to_date(t1.qc_time)        as  stat_date                            
    ,t1.merchant_id	                              
    ,t1.merchant_name	                          
    ,t1.supplier_region_name 	                  
    ,t1.supplier_site_name 	                      
    ,t1.central_warehouse_site_name  	          
    ,t1.b2c_business_mode	                      
    ,t1.cate_name	                              
    ,t1.brand_id	                              
    ,t1.brand_name	                              
    ,t1.model_id	                              
    ,t1.model_name	                              
    ,t1.system	                                  
    ,t1.cargo_tray_classify 	                  
    ,t1.spec_appearance_quality	                  
    ,t1.spec_function_quality	                  
    ,t1.actual_post_type_name
    ,count(  distinct t1.qc_code )      as  qc_num
from  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005  t1  --  基础商品信息
where    to_date(t1.qc_time)   >='2025-02-01' 
group by   
    to_date(t1.qc_time)                                
    ,t1.merchant_id	                              
    ,t1.merchant_name	                          
    ,t1.supplier_region_name 	                  
    ,t1.supplier_site_name 	                      
    ,t1.central_warehouse_site_name  	          
    ,t1.b2c_business_mode	                      
    ,t1.cate_name	                              
    ,t1.brand_id	                              
    ,t1.brand_name	                              
    ,t1.model_id	                              
    ,t1.model_name	                              
    ,t1.system	                                  
    ,t1.cargo_tray_classify 	                  
    ,t1.spec_appearance_quality	                  
    ,t1.spec_function_quality	                  
    ,t1.actual_post_type_name	;  






DROP TABLE IF EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_008;
CREATE TABLE IF NOT EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_008 AS
with qc_review  as (
select 
     audit_time
    ,UNIX_TIMESTAMP(audit_time)           AS audit_time_timestamp_unix
    ,qc_code
    ,if( error_type like '%主板图%' and  audit_status_id <> 1 ,1,0  )   as is_review_pass   --  未通过 
from 
     hdp_zhuanzhuan_dw_global.dw_perform_qc_substation_audit_detail_full_1d 
where dt = '2026-04-15' 
   and  is_del  = 0  -- 代表未删除     
   and main_board_approver_status  = 2 -- 代表已计算
   and pg_cate_id  = 101  -- 代表品类为手机
   and to_date(audit_time)  >= '2025-02-01'  --  限制时间为 2025.02.01
) 

select 
    to_date(t1.audit_time)          as stat_date                         
    ,t2.merchant_id	                              
    ,t2.merchant_name	                          
    ,t2.supplier_region_name 	                  
    ,t2.supplier_site_name 	                      
    ,t2.central_warehouse_site_name  	          
    ,t2.b2c_business_mode	                      
    ,t2.cate_name	                              
    ,t2.brand_id	                              
    ,t2.brand_name	                              
    ,t2.model_id	                              
    ,t2.model_name	                              
    ,t2.system	                                  
    ,t2.cargo_tray_classify 	                  
    ,t2.spec_appearance_quality	                  
    ,t2.spec_function_quality	                  
    ,t2.actual_post_type_name
    ,count(  distinct   t1.qc_code )                                        as  motherboard_image_review_num
    ,count(  distinct if(  t1.is_review_pass  = 1 , t1.qc_code,null ) )   as  motherboard_image_review_reject_num
from 
    qc_review  t1 
join 
    hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005  t2
on  t1.qc_code = t2.qc_code    
where    t2.online_timestamp_unix  > t1.audit_time_timestamp_unix   -- 先审核  在上架 
group by 
    to_date(t1.audit_time)                                
    ,t2.merchant_id	                              
    ,t2.merchant_name	                          
    ,t2.supplier_region_name 	                  
    ,t2.supplier_site_name 	                      
    ,t2.central_warehouse_site_name  	          
    ,t2.b2c_business_mode	                      
    ,t2.cate_name	                              
    ,t2.brand_id	                              
    ,t2.brand_name	                              
    ,t2.model_id	                              
    ,t2.model_name	                              
    ,t2.system	                                  
    ,t2.cargo_tray_classify 	                  
    ,t2.spec_appearance_quality	                  
    ,t2.spec_function_quality	                  
    ,t2.actual_post_type_name	;  
   


DROP TABLE IF EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_009;
CREATE TABLE IF NOT EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_009 AS
with online_info_detail as (
SELECT 
     t1.info_id
    ,t1.opt_time       
    ,t1.data_type
    ,cast( t1.qc_code  as bigInt )    as qc_code
    ,max(  t2.is_onsale)  over( PARTITION BY t1.qc_code  )                                                                                                     as is_onsale
    ,sum( if(  t1.data_type = 'offline' and  nvl(t2.is_onsale,0)   <> 1 ,1,0 ))   over( PARTITION BY t1.qc_code  order by t1.opt_time   asc )                  as is_next_online
    ,row_number()   over( PARTITION BY t1.qc_code  order by t1.opt_time   asc )                                                                              as rn
FROM 
     hdp_ubu_zhuanzhuan_dw_b2c.dw_info_prod_opt_detail_inc_1d   t1  
join 
     hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005  t2 
on  t1.info_id = t2.info_id 
where  t1.dt >= '2025-02-01'     and    t1.data_type in ('online'  ,'offline')   --  代表上架 或者下架 
)


,qc_abnormal_detail as (
select 
    qc_code
    ,count( distinct qc_pn_name )            as appearance_defect_item_num 
from 
    hdp_zhuanzhuan_dim_global.dim_perform_qc_report_split_detail_full_1d 
where dt = '2026-04-15' 
    and  is_abnormal = 1                   --  代表异常
    and  qc_item_name like  "%外观%"        --  外观异常
    and  pg_cate_id = 101  
group by qc_code

)




,defect_image_detail as (
select 
     cast(qc_code as bigInt )           as qc_code
    ,count( distinct photo_url  )       as defect_image_num
from 
    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_qc_photo_version_full_1d  
where dt = '2026-04-15'  and photo_type = 1     -- 照片类型，1:瑕疵照片
    group by  qc_code

)

select
    to_date( t1.opt_time)         as stat_date                             
    ,t2.merchant_id	                              
    ,t2.merchant_name	                          
    ,t2.supplier_region_name 	                  
    ,t2.supplier_site_name 	                      
    ,t2.central_warehouse_site_name  	          
    ,t2.b2c_business_mode	                      
    ,t2.cate_name	                              
    ,t2.brand_id	                              
    ,t2.brand_name	                              
    ,t2.model_id	                              
    ,t2.model_name	                              
    ,t2.system	                                  
    ,t2.cargo_tray_classify 	                  
    ,t2.spec_appearance_quality	                  
    ,t2.spec_function_quality	                  
    ,t2.actual_post_type_name
    ,count(  distinct  if(  t1.data_type = 'online',t1.qc_code,null   ) )	                                          as  shelf_on_num
    ,count(  distinct  if(  t1.data_type = 'offline'  and  nvl(t1.is_onsale,0)  <> 1  ,t1.qc_code,null   ) )	      as  shelf_off_num
    ,count(  distinct  if(  t1.data_type = 'online'   and  nvl(t1.is_next_online,0)  >=  1   ,t1.qc_code,null   ) )	  as  re_shelf_on_num
    ,sum( if(  t1.rn = 1 , t3.appearance_defect_item_num,null))                                                        as appearance_defect_item_num
    ,sum( if(  t1.rn = 1 , t4.defect_image_num,null))                                                                  as defect_image_num
from 
      online_info_detail    t1 
left join      
       hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005    t2 
on   t1.info_id = t2.info_id  
left join
     qc_abnormal_detail    t3   
on   t1.qc_code = t3.qc_code
left join
      defect_image_detail   t4
on  t1.qc_code = t4.qc_code   
group by      
     to_date(t1.opt_time)                                
    ,t2.merchant_id	                              
    ,t2.merchant_name	                          
    ,t2.supplier_region_name 	                  
    ,t2.supplier_site_name 	                      
    ,t2.central_warehouse_site_name  	          
    ,t2.b2c_business_mode	                      
    ,t2.cate_name	                              
    ,t2.brand_id	                              
    ,t2.brand_name	                              
    ,t2.model_id	                              
    ,t2.model_name	                              
    ,t2.system	                                  
    ,t2.cargo_tray_classify 	                  
    ,t2.spec_appearance_quality	                  
    ,t2.spec_function_quality	                  
    ,t2.actual_post_type_name	;  





DROP TABLE IF EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0010;
CREATE TABLE IF NOT EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0010 AS
with  hy_qc_order_detail as (
select 
     cast( t1.qc_code as bigInt)      as qc_code
    ,t1.finish_time
    ,unix_timestamp(  t1.finish_time )                                                                                                     as finish_time_timestamp_unix
    ,if( t1.is_intercept = '是', 1,0)                                                                                                       as is_hy_intercept_pass
    ,case when  t1.is_intercept = '是' and  t1.is_diff = '是'  and  t1.photo_list_show_tile  like "%外观%"  then '外观'
          when  t1.is_intercept = '是' and  t1.is_diff = '是'  and  t1.photo_list_show_tile  like "%维修%"  then '维修'
          when  t1.is_intercept = '是' and  t1.is_diff = '是'  and  nvl(t1.photo_list_show_tile,'') <> ''  
              and     t1.photo_list_show_tile  NOT RLIKE '维修|外观'                                             then '功能'
          when  t1.is_intercept = '是'     then '其他'
          else '' end    as                hy_intercept_type 

from 
(
   select 
       t1.qc_code
      ,t1.finish_time
      ,t1.is_intercept
      ,t1.is_diff
      ,t1.photo_list_show_tile
   from 
      hdp_zhuanzhuan_dm_global.dm_perform_qc_after_sell_diff_ot_detail_all_inc_1d t1 
   where t1.dt >=  '2025-02-01'   and t1.check_result !='入仓暂存'   and   to_date(t1.finish_time) >= '2025-02-01'   
)      t1  
join 
(
    select 
         qc_code
    from 
        hdp_zhuanzhuan_dim_global.dim_perform_qc_report_split_detail_full_1d 
    where dt = '2026-04-15'  and pg_cate_id = 101 --  代表品类为手机     
        group by  1
) t2   
on   cast(t1.qc_code  as bigInt) = t2.qc_code    -- 只保留品类为手机 
 


)


,kf_interfere_ask   as (
select 
     cast( t3.qc_code  as bigInt )              as qc_code
    ,t2.call_start_time                         as call_start_time
    ,unix_timestamp( t2.call_start_time )       as call_start_time_timestamp_unix
FROM 
     hdp_zhuanzhuan_dw_global.dw_perform_kf_ticket_wide_full_1d  t1 
join 
    hdp_zhuanzhuan_dw_global.dw_perform_kf_artificial_call_detail_full_1d   t2 
on  t1.work_order_id = t2.work_order_id and  t2.dt = '2026-04-15'   
left join  
     hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d  t3 
on   cast( t1.order_id  as bigint )  = t3.order_id and  t3.dt = '2026-04-15' 
where t1.dt = '2026-04-15'
    and  t1.is_call_out  = 1     --  代表客服呼出
    and  t1.opt_type_name in ('用户选择发货' ,'客服代用户选择发货')  --  代表用户选择发货 或者客服代用户选择发货
    and  t2.call_type_id  = 2     --  表示 客服呼出
    and  t2.call_result_id =  0   -- 表示电话接通  
    and  t3.cate_first_id = 101  -- 代表手机品类
group by 1,2,3    
)



,is_kf_kf_interfere_ask as (
select 
     t1.qc_code
    ,ROW_NUMBER() over( PARTITION BY  t1.qc_code  order by  t2.call_start_time_timestamp_unix  - t1.finish_time_timestamp_unix    asc )   as rn
from 
    hy_qc_order_detail  t1 
join 
    kf_interfere_ask    t2
on   t1.qc_code = t2.qc_code
where  t2.call_start_time_timestamp_unix   >=  t1.finish_time_timestamp_unix    --完成后验差异拦截 后 一般客服再介入   
having rn = 1    --  只保留最近一次
)


,hy_info_base_info as (
select 
      t1.qc_code 
     ,t3.merchant_id	                              
     ,t3.merchant_name	                          
     ,t3.supplier_region_name 	                  
     ,t3.supplier_site_name 	                      
     ,t3.central_warehouse_site_name  	          
     ,t3.b2c_business_mode	                      
     ,t3.cate_name	                              
     ,t3.brand_id	                              
     ,t3.brand_name	                              
     ,t3.model_id	                              
     ,t3.model_name	                              
     ,t3.system	                                  
     ,t3.cargo_tray_classify 	                  
     ,t3.spec_appearance_quality	                  
     ,t3.spec_function_quality	                  
     ,t3.actual_post_type_name
     ,ROW_NUMBER()  over( PARTITION BY   t1.qc_code  order by ( t1.finish_time_timestamp_unix -  t3.online_timestamp_unix ) asc )   as rn
from 
   hy_qc_order_detail  t1 
 left join
  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005  t3
on   t1.qc_code = t3.qc_code
where  t1.finish_time_timestamp_unix   >=  t3.online_timestamp_unix  --  先上架 后售后
)



select 
    to_date(t1.finish_time)       as stat_date                           
    ,t1.merchant_id	                              
    ,t1.merchant_name	                          
    ,t1.supplier_region_name 	                  
    ,t1.supplier_site_name 	                      
    ,t1.central_warehouse_site_name  	          
    ,t1.b2c_business_mode	                      
    ,t1.cate_name	                              
    ,t1.brand_id	                              
    ,t1.brand_name	                              
    ,t1.model_id	                              
    ,t1.model_name	                              
    ,t1.system	                                  
    ,t1.cargo_tray_classify 	                  
    ,t1.spec_appearance_quality	                  
    ,t1.spec_function_quality	                  
    ,t1.actual_post_type_name
    ,count( distinct t1.qc_code )                                                                            as   post_inspection_num
    ,count( distinct if( t1.is_hy_intercept_pass = 1 ,t1.qc_code,null ) )                                    as   post_intercept_num
    ,count( distinct if( t1.is_hy_intercept_pass = 1 and  t1.is_kf_intercept_pass = 1  ,t1.qc_code,null  ) )       as   post_intercept_success_num
    ,count( distinct if( t1.is_hy_intercept_pass = 1 and  t1.hy_intercept_type = '外观',t1.qc_code,null ) )   as   post_appearance_intercept_num
    ,count( distinct if( t1.is_hy_intercept_pass = 1 and  t1.hy_intercept_type = '维修',t1.qc_code,null) )    as   post_repair_intercept_num
    ,count( distinct if( t1.is_hy_intercept_pass = 1 and  t1.hy_intercept_type = '功能',t1.qc_code,null) )    as   post_function_intercept_num
    ,count( distinct if( t1.is_hy_intercept_pass = 1 and  t1.hy_intercept_type = '其他',t1.qc_code,null) )    as   post_other_intercept_num
from 
(
    select
         t1.finish_time
        ,t3.merchant_id	                              
        ,t3.merchant_name	                          
        ,t3.supplier_region_name 	                  
        ,t3.supplier_site_name 	                      
        ,t3.central_warehouse_site_name  	          
        ,t3.b2c_business_mode	                      
        ,t3.cate_name	                              
        ,t3.brand_id	                              
        ,t3.brand_name	                              
        ,t3.model_id	                              
        ,t3.model_name	                              
        ,t3.system	                                  
        ,t3.cargo_tray_classify 	                  
        ,t3.spec_appearance_quality	                  
        ,t3.spec_function_quality	                  
        ,t3.actual_post_type_name
        ,t1.qc_code
        ,t1.is_hy_intercept_pass
        ,t1.hy_intercept_type
        ,if( t2.qc_code is not null,1,0)                                                                   as   is_kf_intercept_pass
    from 
         hy_qc_order_detail   t1 
    left join 
         is_kf_kf_interfere_ask    t2 
    on  t1.qc_code = t2.qc_code  and t2.rn = 1
    left join 
         hy_info_base_info     t3
    on   t1.qc_code = t3.qc_code  and t3.rn = 1
) t1 
group by  
     to_date(t1.finish_time)                                
    ,t1.merchant_id	                              
    ,t1.merchant_name	                          
    ,t1.supplier_region_name 	                  
    ,t1.supplier_site_name 	                      
    ,t1.central_warehouse_site_name  	          
    ,t1.b2c_business_mode	                      
    ,t1.cate_name	                              
    ,t1.brand_id	                              
    ,t1.brand_name	                              
    ,t1.model_id	                              
    ,t1.model_name	                              
    ,t1.system	                                  
    ,t1.cargo_tray_classify 	                  
    ,t1.spec_appearance_quality	                  
    ,t1.spec_function_quality	                  
    ,t1.actual_post_type_name	;  






DROP TABLE IF EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0011;
CREATE TABLE IF NOT EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0011 AS
--  抽检 预分配
with  pre_allocation_view  (
select 
     qc_code
    ,cast( list_id  as bigInt)           as list_id
from 
    hdp_zhuanzhuan_rawdb_global.raw_mysql_tdb_qc_digitization_qc_spot_check_pre_allocate_list_flow_full_1d  
lateral view explode(split(regexp_replace(list_ids, '^\\\[|\\\]$', ''), ',')) list_ids  as list_id  
where  dt = '2026-04-15'  
     group by 1,2
)

-- 代表后验抽检拆机
,qc_sampling_detail_view  as (
select 
     t2.qc_code
from 
     hdp_zhuanzhuan_dw_global.dw_perform_qc_qa_list_strategy_dtl_full_1d  t1
join 
   pre_allocation_view         t2 
on t1.list_id  = t2.list_id
    and t1.dt = '2026-04-15'    
      and  t1.list_delete_flag  = 0   -- 代表未删除
      and  t1.rule_type_id   = 8   --   个体评估-商户
group by 1  
)




-- 代表ai 拆机
,ai_qc_sampling_detail_view  as (
select 
    t1.qc_code
from 
    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_ai_xray_photo_recognition_full_1d   t1 
where  t1.dt = '2026-04-15'  and get_json_object(t1.process_result, '$.needDismantle') = 'true'  --  代表需要拆机     
    group by 1
)



--  定义手机模块 不进入卖场
,qc_condition_rule_view  as (
select
    t2.item_ids      as  item_id
from 
    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_brain_qc_condition_rule_full_1d  t1 
left join 
     hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_brain_qc_condition_rule_detail_full_1d   t2 
on  t1.id = t2.rule_id and  t2.dt = '2026-04-15'     
where t1.dt = '2026-04-15'  
    and  t1.id = 1  -- 1  这条规则是手机通用等级规则 
    and  t1.deleted = 0      --  代表未删除
    and  t2.dict_id = 32     -- 32 新成色
    and  t2.condition_level =  1145143  ---表示不进入卖场
group by 1  
)




--- 获取后验报告非拆修项(一级item_id != 44) 命中等级「不进卖场」的项
-- , 和 一级item_id = 44, 三级项展示名包含XRay项命中等级「不进卖场」的项
,qc_no_sales_floor  as (
select
    t1.qc_code  
from 
      hdp_zhuanzhuan_dw_global.dw_perform_qc_after_sell_diff_detail_full_1d  t1
join 
    qc_condition_rule_view   t2 
on   t1.new_third_item_id  =  t2.item_id    
where t1.dt = '2026-04-15'    and (  t1.first_item_id != 44    or    t1.new_third_item_name  regexp 'Xray'    )
group by t1.qc_code
)



-- 获取报损数据
, qc_damaged_detail_view  as  (
select 
    qc_code
from 
    hdp_zhuanzhuan_dw_global.dw_perform_qc_damaged_detail_full_1d
where dt = '2026-04-15'  
    group by 1 
)

,hy_qc_detail_view  as (

select 
     t1.qc_code
    ,t1.finish_time
    ,t1.intercept_state_id
    ,t1.first_item_id
    ,t1.second_item_id
    ,t1.new_third_item_id
    ,t1.ori_third_item_id
    ,t1.child_item_is_diff
    ,t1.unable_qc_reason
from 
(
    select 
         qc_code
        ,finish_time
        ,intercept_state_id
        ,first_item_id
        ,second_item_id
        ,new_third_item_id
        ,ori_third_item_id
        ,child_item_is_diff
        ,unable_qc_reason
    from
        hdp_zhuanzhuan_dw_global.dw_perform_qc_after_sell_diff_detail_full_1d
    where dt = '2026-04-15' and  to_date(finish_time) >= '2025-02-01'  and intercept_state_id <> 5  -- 剔除入仓暂存
) t1 
join 
(
    select 
         qc_code
    from 
        hdp_zhuanzhuan_dim_global.dim_perform_qc_report_split_detail_full_1d 
    where dt = '2026-04-15'  and pg_cate_id = 101 --  代表品类为手机     
        group by  1
) t2 
on t1.qc_code = t2.qc_code
left  join 
(
    select 
         qc_code
    from 
         hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_qc_after_sell_req_info_full_1d 
    where dt = '2026-04-15'  
        group by 1
) t4
on t1.qc_code = t4.qc_code   
where if( t1.intercept_state_id = 0 and   t4.qc_code is null ,1,0) = 0  -- 剔除免检机器    -- 只关注实际走后验的单量

)



--- 计算
,hy_zc_qc_detail_view  as (
select 
    t1.qc_code
from 
(

select 
     t1.qc_code
    ,t2.diff_third_item_id
from
(

    select 
         t1.qc_code
         ,array_join(
            array_except(
                split(t1.new_third_item_id, ','), 
                split(nvl(t1.ori_third_item_id,'') , ',')
            ), 
                ','
            ) AS diff_third_item_id
    from 
          hy_qc_detail_view  t1
    where    t1.first_item_id = 44 
        and  t1.second_item_id    not in  (187869, 187870, 176962)  -- 剔除 187869, 187870, 176962 这三个二级item_id
        and  t1.child_item_is_diff = 1  -- 代表有差异
        and  t1.new_third_item_id  is  not null  -- 代表后验三级项·
) t1 
lateral view explode(split(diff_third_item_id, ','))  t2   as diff_third_item_id
) t1 
join 
   hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_brain_qc_item_extend_full_1d t2 
on  cast( t1.diff_third_item_id  as bigInt)   =  t2.item_id  and t2.is_abnormal  = 1   and  t2.dt = '2026-04-15'
group  by  1 

)



select 
    t1.qc_code
   ,t1.finish_time
   ,if( t2.qc_code is null and t3.qc_code is null ,1,0)                                                           as is_hy_cj_qc
   ,if( t1.second_item_id = 176962 and t1.intercept_state_id != 2 
       and   ( t1.new_third_item_id in (176963,176964)  or  t1.unable_qc_reason regexp  '深拆|浅拆' ) ,1,0 )     as is_hy_cj_qc_reject
   ,if( t4.qc_code is not null,1,0)                                                                              as is_no_sales_floor
   ,if( t5.qc_code is not null,1,0)                                                                              as is_damaged
   ,if( t6.qc_code is not null,1,0)                                                                              as is_hy_zc_qc
   ,unix_timestamp(t1.finish_time)                                                                              as finish_time_timestamp_unix
from 
    hy_qc_detail_view  t1 
left join 
    qc_sampling_detail_view t2 
on  t1.qc_code = t2.qc_code
left join
    ai_qc_sampling_detail_view t3 
on  t1.qc_code = t3.qc_code 

left join
    qc_no_sales_floor  t4
on  t1.qc_code = t4.qc_code

left join 
    qc_damaged_detail_view  t5
on  t1.qc_code = t5.qc_code

left join
    hy_zc_qc_detail_view  t6
on  t1.qc_code = t6.qc_code ;




DROP TABLE IF EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0012;
CREATE TABLE IF NOT EXISTS hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0012 AS
select 
    to_date(t1.finish_time)    as stat_date                              
    ,t2.merchant_id	                              
    ,t2.merchant_name	                          
    ,t2.supplier_region_name 	                  
    ,t2.supplier_site_name 	                      
    ,t2.central_warehouse_site_name  	          
    ,t2.b2c_business_mode	                      
    ,t2.cate_name	                              
    ,t2.brand_id	                              
    ,t2.brand_name	                              
    ,t2.model_id	                              
    ,t2.model_name	                              
    ,t2.system	                                  
    ,t2.cargo_tray_classify 	                  
    ,t2.spec_appearance_quality	                  
    ,t2.spec_function_quality	                  
    ,t2.actual_post_type_name
    ,count( distinct if( t1.is_hy_cj_qc = 0  and t1.is_hy_cj_qc_reject =  1 and t1.is_no_sales_floor  = 0  ,t1.qc_code ,null) )                             as actual_disassembly_num 
    ,count( distinct if( t1.is_hy_cj_qc = 0  and t1.is_hy_cj_qc_reject =  1 and t1.is_no_sales_floor  = 0 and  t1.is_hy_zc_qc = 1 ,t1.qc_code ,null )  )    as disassembly_found_num 
    ,count( distinct if( t1.is_damaged = 1  ,t1.qc_code ,null  )  )                                                                                        as disassembly_damage_num 
from 
   hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0011    t1 
left join 
   hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_005     t2  
on  t1.qc_code = t2.qc_code
where  t1.finish_time_timestamp_unix  > t2.online_timestamp_unix  --  先上架  后后验     
group by   
    to_date(t1.finish_time)                                
    ,t2.merchant_id	                              
    ,t2.merchant_name	                          
    ,t2.supplier_region_name 	                  
    ,t2.supplier_site_name 	                      
    ,t2.central_warehouse_site_name  	          
    ,t2.b2c_business_mode	                      
    ,t2.cate_name	                              
    ,t2.brand_id	                              
    ,t2.brand_name	                              
    ,t2.model_id	                              
    ,t2.model_name	                              
    ,t2.system	                                  
    ,t2.cargo_tray_classify 	                  
    ,t2.spec_appearance_quality	                  
    ,t2.spec_function_quality	                  
    ,t2.actual_post_type_name	;  

 


insert overwrite table hdp_zhuanzhuan_ads_global.ads_bi_bds_quality_control_dashboard_full_1d  
partition ( dt='2026-04-15' )


SELECT 
     t1.stat_date	                          
    ,t1.merchant_id	                          
    ,t1.merchant_name	                      
    ,t1.supplier_region_name 	              
    ,t1.supplier_site_name 	                  
    ,t1.central_warehouse_site_name  	      
    ,t1.b2c_business_mode	                  
    ,t1.cate_name	                          
    ,t1.brand_id	                          
    ,t1.brand_name	                          
    ,t1.model_id	                          
    ,t1.model_name	                          
    ,t1.system	                              
    ,t1.cargo_tray_classify 	              
    ,t1.spec_appearance_quality	              
    ,t1.spec_function_quality	              
    ,t1.actual_post_type_name	              
    ,sum( t1.sign_order_num	                           )          as  sign_order_num	                      
    ,sum( t1.appearance_issue_order_num	               )          as  appearance_issue_order_num	          
    ,sum( t1.function_issue_order_num	               )          as  function_issue_order_num	          
    ,sum( t1.four_day_appearance_issue_order_num       )          as  four_day_appearance_issue_order_num	  
    ,sum( t1.four_day_function_issue_order_num 	       )          as  four_day_function_issue_order_num 	  
    ,sum( t1.nps_survey_order_num	                   )          as  nps_survey_order_num	              
    ,sum( t1.nps_survey_promoter_order_num	           )          as  nps_survey_promoter_order_num	      
    ,sum( t1.nps_survey_neutral_order_num	           )          as  nps_survey_neutral_order_num	      
    ,sum( t1.nps_survey_detractor_order_num	           )          as  nps_survey_detractor_order_num	      
    ,sum( t1.qc_num	                                   )          as  qc_num	                              
    ,sum( t1.motherboard_image_review_num	           )          as  motherboard_image_review_num	      
    ,sum( t1.motherboard_image_review_reject_num       )          as  motherboard_image_review_reject_num	  
    ,sum( t1.shelf_on_num	                           )          as  shelf_on_num	                      
    ,sum( t1.shelf_off_num	                           )          as  shelf_off_num	                      
    ,sum( t1.re_shelf_on_num	                       )          as  re_shelf_on_num	                      
    ,sum( t1.defect_image_num	                       )          as  defect_image_num	                  
    ,sum( t1.appearance_defect_item_num	               )          as  appearance_defect_item_num	          
    ,sum( t1.post_inspection_num	                   )          as  post_inspection_num	                  
    ,sum( t1.post_intercept_num	                       )          as  post_intercept_num	                  
    ,sum( t1.post_intercept_success_num	               )          as  post_intercept_success_num	          
    ,sum( t1.post_appearance_intercept_num	           )          as  post_appearance_intercept_num	      
    ,sum( t1.post_repair_intercept_num	               )          as  post_repair_intercept_num	          
    ,sum( t1.post_function_intercept_num	           )          as  post_function_intercept_num	          
    ,sum( t1.post_other_intercept_num  	               )          as  post_other_intercept_num  	          
    ,sum( t1.actual_disassembly_num	                   )          as  actual_disassembly_num	              
    ,sum( t1.disassembly_found_num 	                   )          as  disassembly_found_num 	              
    ,sum( t1.disassembly_damage_num  	               )          as  disassembly_damage_num  	          
FROM 
(

    select     
         nvl(stat_date	                    ,'')     as     stat_date	                                     
        ,nvl(merchant_id	                ,-9)     as     merchant_id	                                 
        ,nvl(merchant_name	                ,'')     as     merchant_name	                                 
        ,nvl(supplier_region_name 	        ,'')     as     supplier_region_name 	                         
        ,nvl(supplier_site_name 	        ,'')     as     supplier_site_name 	                         
        ,nvl(central_warehouse_site_name  	,'')     as     central_warehouse_site_name  	                 
        ,nvl(b2c_business_mode	            ,'')     as     b2c_business_mode	                             
        ,nvl(cate_name	                    ,'')     as     cate_name	                                     
        ,nvl(brand_id	                    ,-9)     as     brand_id	                                     
        ,nvl(brand_name	                    ,'')     as     brand_name	                                     
        ,nvl(model_id	                    ,-9)     as     model_id	                                     
        ,nvl(model_name	                    ,'')     as     model_name	                                     
        ,nvl(system	                        ,'')     as     system	                                         
        ,nvl(cargo_tray_classify 	        ,'')     as     cargo_tray_classify 	                         
        ,nvl(spec_appearance_quality	    ,'')     as     spec_appearance_quality	                     
        ,nvl(spec_function_quality	        ,'')     as     spec_function_quality	                         
        ,nvl(actual_post_type_name	        ,'')     as     actual_post_type_name	                         
        ,sign_order_num	                                 
        ,appearance_issue_order_num	                     
        ,function_issue_order_num	                     
        ,four_day_appearance_issue_order_num	         
        ,four_day_function_issue_order_num 	             
        ,0   as  nps_survey_order_num	                         
        ,0   as  nps_survey_promoter_order_num	                 
        ,0   as  nps_survey_neutral_order_num	                 
        ,0   as  nps_survey_detractor_order_num	                 
        ,0   as  qc_num	                                         
        ,0   as  motherboard_image_review_num	                 
        ,0   as  motherboard_image_review_reject_num	         
        ,0   as  shelf_on_num	                                 
        ,0   as  shelf_off_num	                                 
        ,0   as  re_shelf_on_num	                             
        ,0   as  defect_image_num	                             
        ,0   as  appearance_defect_item_num	                     
        ,0   as  post_inspection_num	                         
        ,0   as  post_intercept_num	                             
        ,0   as  post_intercept_success_num	                     
        ,0   as  post_appearance_intercept_num	                 
        ,0   as  post_repair_intercept_num	                     
        ,0   as  post_function_intercept_num	                 
        ,0   as  post_other_intercept_num  	                     
        ,0   as  actual_disassembly_num	                         
        ,0   as  disassembly_found_num 	                         
        ,0   as  disassembly_damage_num  
    from
        hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_003   -- 支付时间统计
    union all 

    select  
        nvl(stat_date	                    ,'')     as     stat_date	                                     
        ,nvl(merchant_id	                ,-9)     as     merchant_id	                                 
        ,nvl(merchant_name	                ,'')     as     merchant_name	                                 
        ,nvl(supplier_region_name 	        ,'')     as     supplier_region_name 	                         
        ,nvl(supplier_site_name 	        ,'')     as     supplier_site_name 	                         
        ,nvl(central_warehouse_site_name  	,'')     as     central_warehouse_site_name  	                 
        ,nvl(b2c_business_mode	            ,'')     as     b2c_business_mode	                             
        ,nvl(cate_name	                    ,'')     as     cate_name	                                     
        ,nvl(brand_id	                    ,-9)     as     brand_id	                                     
        ,nvl(brand_name	                    ,'')     as     brand_name	                                     
        ,nvl(model_id	                    ,-9)     as     model_id	                                     
        ,nvl(model_name	                    ,'')     as     model_name	                                     
        ,nvl(system	                        ,'')     as     system	                                         
        ,nvl(cargo_tray_classify 	        ,'')     as     cargo_tray_classify 	                         
        ,nvl(spec_appearance_quality	    ,'')     as     spec_appearance_quality	                     
        ,nvl(spec_function_quality	        ,'')     as     spec_function_quality	                         
        ,nvl(actual_post_type_name	        ,'')     as     actual_post_type_name	                         
        ,0   as  sign_order_num	                                 
        ,0   as  appearance_issue_order_num	                     
        ,0   as  function_issue_order_num	                     
        ,0   as  four_day_appearance_issue_order_num	         
        ,0   as  four_day_function_issue_order_num 	             
        , nps_survey_order_num	                         
        , nps_survey_promoter_order_num	                 
        , nps_survey_neutral_order_num	                 
        , nps_survey_detractor_order_num	                 
        ,0   as  qc_num	                                         
        ,0   as  motherboard_image_review_num	                 
        ,0   as  motherboard_image_review_reject_num	         
        ,0   as  shelf_on_num	                                 
        ,0   as  shelf_off_num	                                 
        ,0   as  re_shelf_on_num	                             
        ,0   as  defect_image_num	                             
        ,0   as  appearance_defect_item_num	                     
        ,0   as  post_inspection_num	                         
        ,0   as  post_intercept_num	                             
        ,0   as  post_intercept_success_num	                     
        ,0   as  post_appearance_intercept_num	                 
        ,0   as  post_repair_intercept_num	                     
        ,0   as  post_function_intercept_num	                 
        ,0   as  post_other_intercept_num  	                     
        ,0   as  actual_disassembly_num	                         
        ,0   as  disassembly_found_num 	                         
        ,0   as  disassembly_damage_num  
    from
      hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_004   --  nps 问卷时间


    union all 
    select  
        nvl(stat_date	                    ,'')     as     stat_date	                                     
        ,nvl(merchant_id	                ,-9)     as     merchant_id	                                 
        ,nvl(merchant_name	                ,'')     as     merchant_name	                                 
        ,nvl(supplier_region_name 	        ,'')     as     supplier_region_name 	                         
        ,nvl(supplier_site_name 	        ,'')     as     supplier_site_name 	                         
        ,nvl(central_warehouse_site_name  	,'')     as     central_warehouse_site_name  	                 
        ,nvl(b2c_business_mode	            ,'')     as     b2c_business_mode	                             
        ,nvl(cate_name	                    ,'')     as     cate_name	                                     
        ,nvl(brand_id	                    ,-9)     as     brand_id	                                     
        ,nvl(brand_name	                    ,'')     as     brand_name	                                     
        ,nvl(model_id	                    ,-9)     as     model_id	                                     
        ,nvl(model_name	                    ,'')     as     model_name	                                     
        ,nvl(system	                        ,'')     as     system	                                         
        ,nvl(cargo_tray_classify 	        ,'')     as     cargo_tray_classify 	                         
        ,nvl(spec_appearance_quality	    ,'')     as     spec_appearance_quality	                     
        ,nvl(spec_function_quality	        ,'')     as     spec_function_quality	                         
        ,nvl(actual_post_type_name	        ,'')     as     actual_post_type_name	                         
        ,0   as  sign_order_num	                                 
        ,0   as  appearance_issue_order_num	                     
        ,0   as  function_issue_order_num	                     
        ,0   as  four_day_appearance_issue_order_num	         
        ,0   as  four_day_function_issue_order_num 	             
        ,0   as  nps_survey_order_num	                         
        ,0   as  nps_survey_promoter_order_num	                 
        ,0   as  nps_survey_neutral_order_num	                 
        ,0   as  nps_survey_detractor_order_num	                 
        , qc_num	                                         
        ,0   as  motherboard_image_review_num	                 
        ,0   as  motherboard_image_review_reject_num	         
        ,0   as  shelf_on_num	                                 
        ,0   as  shelf_off_num	                                 
        ,0   as  re_shelf_on_num	                             
        ,0   as  defect_image_num	                             
        ,0   as  appearance_defect_item_num	                     
        ,0   as  post_inspection_num	                         
        ,0   as  post_intercept_num	                             
        ,0   as  post_intercept_success_num	                     
        ,0   as  post_appearance_intercept_num	                 
        ,0   as  post_repair_intercept_num	                     
        ,0   as  post_function_intercept_num	                 
        ,0   as  post_other_intercept_num  	                     
        ,0   as  actual_disassembly_num	                         
        ,0   as  disassembly_found_num 	                         
        ,0   as  disassembly_damage_num  
    from
      hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_007   --  质检时间

    union all 
    select   
        nvl(stat_date	                    ,'')     as     stat_date	                                     
        ,nvl(merchant_id	                ,-9)     as     merchant_id	                                 
        ,nvl(merchant_name	                ,'')     as     merchant_name	                                 
        ,nvl(supplier_region_name 	        ,'')     as     supplier_region_name 	                         
        ,nvl(supplier_site_name 	        ,'')     as     supplier_site_name 	                         
        ,nvl(central_warehouse_site_name  	,'')     as     central_warehouse_site_name  	                 
        ,nvl(b2c_business_mode	            ,'')     as     b2c_business_mode	                             
        ,nvl(cate_name	                    ,'')     as     cate_name	                                     
        ,nvl(brand_id	                    ,-9)     as     brand_id	                                     
        ,nvl(brand_name	                    ,'')     as     brand_name	                                     
        ,nvl(model_id	                    ,-9)     as     model_id	                                     
        ,nvl(model_name	                    ,'')     as     model_name	                                     
        ,nvl(system	                        ,'')     as     system	                                         
        ,nvl(cargo_tray_classify 	        ,'')     as     cargo_tray_classify 	                         
        ,nvl(spec_appearance_quality	    ,'')     as     spec_appearance_quality	                     
        ,nvl(spec_function_quality	        ,'')     as     spec_function_quality	                         
        ,nvl(actual_post_type_name	        ,'')     as     actual_post_type_name	                         
        ,0   as  sign_order_num	                                 
        ,0   as  appearance_issue_order_num	                     
        ,0   as  function_issue_order_num	                     
        ,0   as  four_day_appearance_issue_order_num	         
        ,0   as  four_day_function_issue_order_num 	             
        ,0   as  nps_survey_order_num	                         
        ,0   as  nps_survey_promoter_order_num	                 
        ,0   as  nps_survey_neutral_order_num	                 
        ,0   as  nps_survey_detractor_order_num	                 
        ,0   as qc_num	                                         
        ,motherboard_image_review_num	                 
        ,motherboard_image_review_reject_num	         
        ,0   as  shelf_on_num	                                 
        ,0   as  shelf_off_num	                                 
        ,0   as  re_shelf_on_num	                             
        ,0   as  defect_image_num	                             
        ,0   as  appearance_defect_item_num	                     
        ,0   as  post_inspection_num	                         
        ,0   as  post_intercept_num	                             
        ,0   as  post_intercept_success_num	                     
        ,0   as  post_appearance_intercept_num	                 
        ,0   as  post_repair_intercept_num	                     
        ,0   as  post_function_intercept_num	                 
        ,0   as  post_other_intercept_num  	                     
        ,0   as  actual_disassembly_num	                         
        ,0   as  disassembly_found_num 	                         
        ,0   as  disassembly_damage_num  
    from
      hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_008   --  审核时间
    
    union all 
    select   
        nvl(stat_date	                    ,'')     as     stat_date	                                     
        ,nvl(merchant_id	                ,-9)     as     merchant_id	                                 
        ,nvl(merchant_name	                ,'')     as     merchant_name	                                 
        ,nvl(supplier_region_name 	        ,'')     as     supplier_region_name 	                         
        ,nvl(supplier_site_name 	        ,'')     as     supplier_site_name 	                         
        ,nvl(central_warehouse_site_name  	,'')     as     central_warehouse_site_name  	                 
        ,nvl(b2c_business_mode	            ,'')     as     b2c_business_mode	                             
        ,nvl(cate_name	                    ,'')     as     cate_name	                                     
        ,nvl(brand_id	                    ,-9)     as     brand_id	                                     
        ,nvl(brand_name	                    ,'')     as     brand_name	                                     
        ,nvl(model_id	                    ,-9)     as     model_id	                                     
        ,nvl(model_name	                    ,'')     as     model_name	                                     
        ,nvl(system	                        ,'')     as     system	                                         
        ,nvl(cargo_tray_classify 	        ,'')     as     cargo_tray_classify 	                         
        ,nvl(spec_appearance_quality	    ,'')     as     spec_appearance_quality	                     
        ,nvl(spec_function_quality	        ,'')     as     spec_function_quality	                         
        ,nvl(actual_post_type_name	        ,'')     as     actual_post_type_name	                         
        ,0   as  sign_order_num	                                 
        ,0   as  appearance_issue_order_num	                     
        ,0   as  function_issue_order_num	                     
        ,0   as  four_day_appearance_issue_order_num	         
        ,0   as  four_day_function_issue_order_num 	             
        ,0   as  nps_survey_order_num	                         
        ,0   as  nps_survey_promoter_order_num	                 
        ,0   as  nps_survey_neutral_order_num	                 
        ,0   as  nps_survey_detractor_order_num	                 
        ,0   as qc_num	                                         
        ,0   as motherboard_image_review_num	                 
        ,0   as motherboard_image_review_reject_num	         
        , shelf_on_num	                                 
        , shelf_off_num	                                 
        , re_shelf_on_num	                             
        , defect_image_num	                             
        , appearance_defect_item_num	                     
        ,0   as  post_inspection_num	                         
        ,0   as  post_intercept_num	                             
        ,0   as  post_intercept_success_num	                     
        ,0   as  post_appearance_intercept_num	                 
        ,0   as  post_repair_intercept_num	                     
        ,0   as  post_function_intercept_num	                 
        ,0   as  post_other_intercept_num  	                     
        ,0   as  actual_disassembly_num	                         
        ,0   as  disassembly_found_num 	                         
        ,0   as  disassembly_damage_num  
    from
       hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_009   --  上架时间
    
    union all 
    select   
        nvl(stat_date	                    ,'')     as     stat_date	                                     
       ,nvl(merchant_id	                ,-9)     as     merchant_id	                                 
       ,nvl(merchant_name	                ,'')     as     merchant_name	                                 
       ,nvl(supplier_region_name 	        ,'')     as     supplier_region_name 	                         
       ,nvl(supplier_site_name 	        ,'')     as     supplier_site_name 	                         
       ,nvl(central_warehouse_site_name  	,'')     as     central_warehouse_site_name  	                 
       ,nvl(b2c_business_mode	            ,'')     as     b2c_business_mode	                             
       ,nvl(cate_name	                    ,'')     as     cate_name	                                     
       ,nvl(brand_id	                    ,-9)     as     brand_id	                                     
       ,nvl(brand_name	                    ,'')     as     brand_name	                                     
       ,nvl(model_id	                    ,-9)     as     model_id	                                     
       ,nvl(model_name	                    ,'')     as     model_name	                                     
       ,nvl(system	                        ,'')     as     system	                                         
       ,nvl(cargo_tray_classify 	        ,'')     as     cargo_tray_classify 	                         
       ,nvl(spec_appearance_quality	    ,'')     as     spec_appearance_quality	                     
       ,nvl(spec_function_quality	        ,'')     as     spec_function_quality	                         
       ,nvl(actual_post_type_name	        ,'')     as     actual_post_type_name	                         
       ,0   as  sign_order_num	                                 
       ,0   as  appearance_issue_order_num	                     
       ,0   as  function_issue_order_num	                     
       ,0   as  four_day_appearance_issue_order_num	         
       ,0   as  four_day_function_issue_order_num 	             
       ,0   as  nps_survey_order_num	                         
       ,0   as  nps_survey_promoter_order_num	                 
       ,0   as  nps_survey_neutral_order_num	                 
       ,0   as  nps_survey_detractor_order_num	                 
       ,0   as qc_num	                                         
       ,0   as motherboard_image_review_num	                 
       ,0   as motherboard_image_review_reject_num	         
       ,0   as  shelf_on_num	                                 
       ,0   as  shelf_off_num	                                 
       ,0   as  re_shelf_on_num	                             
       ,0   as  defect_image_num	                             
       ,0   as  appearance_defect_item_num	                     
       , post_inspection_num	                         
       , post_intercept_num	                             
       , post_intercept_success_num	                     
       , post_appearance_intercept_num	                 
       , post_repair_intercept_num	                     
       , post_function_intercept_num	                 
       , post_other_intercept_num  	                     
       ,0   as  actual_disassembly_num	                         
       ,0   as  disassembly_found_num 	                         
       ,0   as  disassembly_damage_num  
    from
         hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0010     --  后验完成时间
    
    union all
    select   
         nvl(stat_date	                    ,'')     as     stat_date	                                     
        ,nvl(merchant_id	                ,-9)     as     merchant_id	                                 
        ,nvl(merchant_name	                ,'')     as     merchant_name	                                 
        ,nvl(supplier_region_name 	        ,'')     as     supplier_region_name 	                         
        ,nvl(supplier_site_name 	        ,'')     as     supplier_site_name 	                         
        ,nvl(central_warehouse_site_name  	,'')     as     central_warehouse_site_name  	                 
        ,nvl(b2c_business_mode	            ,'')     as     b2c_business_mode	                             
        ,nvl(cate_name	                    ,'')     as     cate_name	                                     
        ,nvl(brand_id	                    ,-9)     as     brand_id	                                     
        ,nvl(brand_name	                    ,'')     as     brand_name	                                     
        ,nvl(model_id	                    ,-9)     as     model_id	                                     
        ,nvl(model_name	                    ,'')     as     model_name	                                     
        ,nvl(system	                        ,'')     as     system	                                         
        ,nvl(cargo_tray_classify 	        ,'')     as     cargo_tray_classify 	                         
        ,nvl(spec_appearance_quality	    ,'')     as     spec_appearance_quality	                     
        ,nvl(spec_function_quality	        ,'')     as     spec_function_quality	                         
        ,nvl(actual_post_type_name	        ,'')     as     actual_post_type_name	                         
        ,0   as  sign_order_num	                                 
        ,0   as  appearance_issue_order_num	                     
        ,0   as  function_issue_order_num	                     
        ,0   as  four_day_appearance_issue_order_num	         
        ,0   as  four_day_function_issue_order_num 	             
        ,0   as  nps_survey_order_num	                         
        ,0   as  nps_survey_promoter_order_num	                 
        ,0   as  nps_survey_neutral_order_num	                 
        ,0   as  nps_survey_detractor_order_num	                 
        ,0   as qc_num	                                         
        ,0   as motherboard_image_review_num	                 
        ,0   as motherboard_image_review_reject_num	         
        ,0   as  shelf_on_num	                                 
        ,0   as  shelf_off_num	                                 
        ,0   as  re_shelf_on_num	                             
        ,0   as  defect_image_num	                             
        ,0   as  appearance_defect_item_num	                     
        ,0   as  post_inspection_num	                         
        ,0   as  post_intercept_num	                             
        ,0   as  post_intercept_success_num	                     
        ,0   as  post_appearance_intercept_num	                 
        ,0   as  post_repair_intercept_num	                     
        ,0   as  post_function_intercept_num	                 
        ,0   as  post_other_intercept_num  	                     
        , actual_disassembly_num	                         
        , disassembly_found_num 	                         
        , disassembly_damage_num  
    from
      hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260421_0012     --  后验完成时间
    

) t1 
group by  
    t1.stat_date	                          
    ,t1.merchant_id	                          
    ,t1.merchant_name	                      
    ,t1.supplier_region_name 	              
    ,t1.supplier_site_name 	                  
    ,t1.central_warehouse_site_name  	      
    ,t1.b2c_business_mode	                  
    ,t1.cate_name	                          
    ,t1.brand_id	                          
    ,t1.brand_name	                          
    ,t1.model_id	                          
    ,t1.model_name	                          
    ,t1.system	                              
    ,t1.cargo_tray_classify 	              
    ,t1.spec_appearance_quality	              
    ,t1.spec_function_quality	              
    ,t1.actual_post_type_name	  ;



 -- 清除7天前临时表         
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_001 ;   
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_002 ;  
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_003 ;   
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_004 ;
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_005 ;
-- drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_006 ;
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_007 ;
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_008 ;
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_009 ;
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_0010 ;
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_0011 ;
drop table if exists  hdp_zhuanzhuan_tmp_global.tmp_bi_bds_quality_control_dashboard_jxy_20260408_0012 ;
