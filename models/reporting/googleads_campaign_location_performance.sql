{{ config (
    alias = target.database + '_googleads_campaign_location_performance'
)}}

WITH campaigns_data as 
    (SELECT date::date, split_part(geo_target_metro,'/',2)::varchar as location_id, 
        CASE WHEN location_id = 200803 THEN 'Los Angeles, CA'
            WHEN location_id = 200501 THEN 'New York, NY'
            WHEN location_id = 200753 THEN 'Phoenix, AZ'
            WHEN location_id = 200807 THEN 'San Francisco-Oakland-San Jose, CA'
            ELSE 'Other'
        END as location,
        CASE WHEN campaign_name ~* 'Nonbrand' AND location = 'Los Angeles, CA' THEN 'Unbranded Search LA'
            WHEN campaign_name ~* 'Nonbrand' AND location = 'New York, NY' THEN 'Unbranded Search NY'
            WHEN campaign_name ~* 'Nonbrand' AND location = 'Phoenix, AZ' THEN 'Unbranded Search PHX'
            WHEN campaign_name ~* 'Nonbrand' AND location = 'San Francisco-Oakland-San Jose, CA' THEN 'Unbranded Search SF'
            WHEN campaign_name ~* 'PMax' AND location = 'Los Angeles, CA' THEN 'PMax Search LA'
            WHEN campaign_name ~* 'PMax' AND location = 'New York, NY' THEN 'PMax Search NY'
            WHEN campaign_name ~* 'PMax' AND location = 'Phoenix, AZ' THEN 'PMax Search PHX'
            WHEN campaign_name ~* 'PMax' AND location = 'San Francisco-Oakland-San Jose, CA' THEN 'PMax Search SF'
            WHEN campaign_name ~* 'Youtube' AND location = 'Los Angeles, CA' THEN 'Youtube LA'
            WHEN campaign_name ~* 'Youtube' AND location = 'New York, NY' THEN 'Youtube NY'
            WHEN campaign_name ~* 'Youtube' AND location = 'Phoenix, AZ' THEN 'Youtube PHX'
            WHEN campaign_name ~* 'Youtube' AND location = 'San Francisco-Oakland-San Jose, CA' THEN 'Youtube SF'
            WHEN campaign_name ~* 'Demand Gen' AND location = 'Los Angeles, CA' THEN 'Demand Gen LA'
            WHEN campaign_name ~* 'Demand Gen' AND location = 'New York, NY' THEN 'Demand Gen NY'
        END as campaign_type_custom,
        campaign_name, cost_micros::float/1000000::float as spend, impressions, clicks, coalesce(purchases,0) as purchases, coalesce(leads,0) as leads
    FROM 
        (SELECT * FROM {{ source ('googleads_raw','geo_performance_report') }}
        LEFT JOIN (SELECT date, campaign_id, geo_target_metro, geo_target_city, geo_target_state, 
                coalesce(case when conversion_action_name = 'Google Adwords - Pink' then all_conversions end,0) as leads, 
                coalesce(case when conversion_action_name = 'Google Adwords - Raspberry' then all_conversions end,0) as purchases
            FROM {{ source ('googleads_raw','geo_convtype_performance_report') }}
            USING(date,campaign_id,geo_target_metro,geo_target_city,geo_target_state))
    WHERE campaign_name ~* 'all'
    ORDER by date desc),

    campaigns_geo_data as
    (SELECT date::date, location, campaign_type_custom, campaign_name, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks, 
    coalesce(sum(purchases),0) as purchases, coalesce(sum(leads),0) as leads FROM campaigns_data WHERE location != 'Other' GROUP BY 1,2,3,4
    )

SELECT * FROM campaigns_geo_data
UNION ALL
SELECT date::date, 
    CASE WHEN campaign_name ~* 'LA' THEN 'Los Angeles, CA'
        WHEN campaign_name ~* 'NY' THEN 'New York, NY'
        WHEN campaign_name ~* 'PHX' OR campaign_name ~* 'AZ' THEN 'Phoenix, AZ'
        WHEN campaign_name ~* 'SF' THEN 'San Francisco-Oakland-San Jose, CA'
    END as location,
    CASE WHEN campaign_name ~* 'Search_Branded_LA' THEN 'Branded Search LA'
        WHEN campaign_name ~* 'Search_Branded_NY' THEN 'Branded Search NY'
        WHEN campaign_name ~* 'Search_Branded_PHX' OR campaign_name ~* 'Search_Branded_AZ' THEN 'Branded Search PHX'
        WHEN campaign_name ~* 'Search_Branded_SF' THEN 'Branded Search SF'
        WHEN campaign_name ~* 'Nonbrand' AND campaign_name ~* 'LA' THEN 'Unbranded Search LA'
        WHEN campaign_name ~* 'Nonbrand' AND campaign_name ~* 'NY' THEN 'Unbranded Search NY'
        WHEN campaign_name ~* 'Nonbrand' AND (campaign_name ~* 'PHX' OR campaign_name ~* 'AZ') THEN 'Unbranded Search PHX'
        WHEN campaign_name ~* 'Nonbrand' AND campaign_name ~* 'SF' THEN 'Unbranded Search SF'
        WHEN campaign_name ~* 'PMax' AND campaign_name ~* 'LA' THEN 'PMax Search LA'
        WHEN campaign_name ~* 'PMax' AND campaign_name ~* 'NY' THEN 'PMax Search NY'
        WHEN campaign_name ~* 'PMax' AND (campaign_name ~* 'PHX' OR campaign_name ~* 'AZ') THEN 'PMax Search PHX'
        WHEN campaign_name ~* 'PMax' AND campaign_name ~* 'SF' THEN 'PMax Search SF'
        WHEN campaign_name ~* 'Youtube' AND campaign_name ~* 'LA' THEN 'Youtube LA'
        WHEN campaign_name ~* 'Youtube' AND campaign_name ~* 'NY' THEN 'Youtube NY'
        WHEN campaign_name ~* 'Youtube' AND (campaign_name ~* 'PHX' OR campaign_name ~* 'AZ') THEN 'Youtube PHX'
        WHEN campaign_name ~* 'Youtube' AND campaign_name ~* 'SF' THEN 'Youtube SF'
        WHEN campaign_name ~* 'Discovery' AND campaign_name ~* 'LA' THEN 'Discovery LA'
        WHEN campaign_name ~* 'Demand Gen' AND campaign_name ~* 'LA' THEN 'Demand Gen LA'
        WHEN campaign_name ~* 'Demand Gen' AND campaign_name ~* 'NY' THEN 'Demand Gen NY'
        ELSE 'Other'
    END as campaign_type_custom,
    campaign_name, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks, sum(purchases) as purchases, sum(leads) as leads
FROM {{ ref('googleads_campaign_performance') }}
WHERE campaign_name !~* 'ALL'
and date_granularity = 'day'
GROUP BY 1,2,3,4
