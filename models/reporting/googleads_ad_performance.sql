{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
ad_name,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN campaign_name ~* 'Search_Branded_LA' THEN 'Branded Search LA'
    WHEN campaign_name ~* 'Search_Branded_NY' THEN 'Branded Search NY'
    WHEN campaign_name ~* 'Search_Branded_PHX' OR campaign_name ~* 'Search_Branded_AZ' THEN 'Branded Search PHX'
    WHEN campaign_name ~* 'Search_Branded_SF' THEN 'Branded Search SF'
    WHEN (campaign_name ~* 'Search_Nonbrand' OR campaign_name ~* 'PMax') AND campaign_name ~* 'LA' AND campaign_name !~* 'ALL' THEN 'Unbranded Search LA'
    WHEN (campaign_name ~* 'Search_Nonbrand' OR campaign_name ~* 'PMax') AND campaign_name ~* 'NY' AND campaign_name !~* 'ALL' THEN 'Unbranded Search NY'
    WHEN (campaign_name ~* 'Search_Nonbrand' OR campaign_name ~* 'PMax') AND (campaign_name ~* 'PHX' OR campaign_name ~* 'AZ') AND campaign_name !~* 'ALL' THEN 'Unbranded Search PHX'
    WHEN (campaign_name ~* 'Search_Nonbrand' OR campaign_name ~* 'PMax') AND campaign_name ~* 'SF' AND campaign_name !~* 'ALL' THEN 'Unbranded Search SF'
    WHEN campaign_name ~* 'Youtube' AND campaign_name ~* 'LA' AND campaign_name !~* 'reach' AND campaign_name !~* 'ALL' THEN 'Youtube LA'
    WHEN campaign_name ~* 'Youtube' AND campaign_name ~* 'NY' AND campaign_name !~* 'reach' AND campaign_name !~* 'ALL' THEN 'Youtube NY'
    WHEN campaign_name ~* 'Youtube' AND (campaign_name ~* 'PHX' OR campaign_name ~* 'AZ') AND campaign_name !~* 'reach' AND campaign_name !~* 'ALL' THEN 'Youtube PHX'
    WHEN campaign_name ~* 'Youtube' AND campaign_name ~* 'SF' AND campaign_name !~* 'reach' AND campaign_name !~* 'ALL' THEN 'Youtube SF'
    WHEN campaign_name ~* 'Discovery' AND campaign_name ~* 'LA' AND campaign_name !~* 'ALL' THEN 'Discovery LA'
    ELSE 'Other'
END as campaign_type_custom,
CASE WHEN campaign_name ~* 'LA' THEN 'LA'
    WHEN campaign_name ~* 'NY' THEN 'NY'
    WHEN campaign_name ~* 'PHX' OR campaign_name ~* 'AZ' THEN 'PHX'
    WHEN campaign_name ~* 'SF' THEN 'SF'
    ELSE 'Other'
END as location,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
raspberry as purchases,
conversions_value as revenue,
pink as leads
FROM {{ ref('googleads_performance_by_ad') }}
LEFT JOIN
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity, customer_id as account_id, ad_id, ad_group_id, campaign_id,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Pink' THEN all_conversions END),0) as pink,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Raspberry' THEN all_conversions END),0) as raspberry
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity, customer_id as account_id, ad_id, ad_group_id, campaign_id,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Pink' THEN all_conversions END),0) as pink,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Raspberry' THEN all_conversions END),0) as raspberry
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity, customer_id as account_id, ad_id, ad_group_id, campaign_id,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Pink' THEN all_conversions END),0) as pink,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Raspberry' THEN all_conversions END),0) as raspberry
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity, customer_id as account_id, ad_id, ad_group_id, campaign_id,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Pink' THEN all_conversions END),0) as pink,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Raspberry' THEN all_conversions END),0) as raspberry
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity, customer_id as account_id, ad_id, ad_group_id, campaign_id,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Pink' THEN all_conversions END),0) as pink,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Raspberry' THEN all_conversions END),0) as raspberry
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6)
    USING(date, date_granularity, account_id, ad_id, ad_group_id, campaign_id)
