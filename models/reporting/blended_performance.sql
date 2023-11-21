{{ config (
    alias = target.database + '_blended_performance'
)}}

SELECT date, 'Facebook' as channel, date_granularity, location as market, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, 
    COALESCE(SUM(link_clicks),0) as clicks, COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases
FROM {{ source('reporting','facebook_ad_performance') }}
GROUP BY 1,2,3,4
    
UNION ALL

SELECT date, 'Google' as channel, date_granularity, location as market, 
    COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases
FROM {{ source('reporting','googleads_campaign_performance') }}
GROUP BY 1,2,3,4

UNION ALL

SELECT date, 'Youtube' as channel, date_granularity, location as market, 
    COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases
FROM {{ source('reporting','googleads_campaign_performance') }}
WHERE campaign_name ~* 'Youtube'
GROUP BY 1,2,3,4

UNION ALL

SELECT date, 'Google Excl. YT' as channel, date_granularity, location as market, 
    COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases
FROM {{ source('reporting','googleads_campaign_performance') }}
WHERE campaign_name !~* 'Youtube'
GROUP BY 1,2,3,4

UNION ALL

SELECT date, 'TikTok' as channel, date_granularity, campaign_type_custom as market, 
    COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases
FROM {{ source('reporting','tiktok_ad_performance') }}
GROUP BY 1,2,3,4
