

insert overwrite table hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_t_account_statement_recycle_detail_full_1d PARTITION(dt = '${outFileSuffix}')
select
    id,
    recycle_order_id,
    recycle_source,
    cate_name,
    brand_name,
    model_name,
    pay_amount,
    coupon_amount,
    raised_price_cost,
    achievements,
    recycle_create_time,
    recycle_payment_time,
    franchisee_profit,
    clerk_profit,
    is_returnd,
    employee_uid,
    store_id,
    franchisee_id,
    return_time,
    store_director_profit_provider,
    store_director_profit_provider_conf,
    collection_status,
    statement_id,
    statement_type,
    personnel_ownership,
    is_commission,
    create_time,
    update_time
from
    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_store_bds_t_account_statement_recycle_full_1d
where dt = '${outFileSuffix}'


