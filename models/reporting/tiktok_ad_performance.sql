{{ config (
    alias = target.database + '_tiktok_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE
    WHEN campaign_name ~* 'LA' OR adgroup_name ~* '- LA' THEN 'LA'
    WHEN campaign_name ~* 'NY' OR adgroup_name ~* '- NY' THEN 'NY'
    WHEN campaign_name ~* 'PHX' OR campaign_name ~* 'AZ' OR adgroup_name ~* '- PHX' OR adgroup_name ~* '- AZ' THEN 'PHX'
    WHEN campaign_name ~* 'SF' OR adgroup_name ~* '- SF' THEN 'SF'
    ELSE 'Other'
END as campaign_type_custom,
adgroup_name,
adgroup_id,
adgroup_status,
audience,
CASE
    WHEN ad_name ~* 'Ostrich' THEN 'Ostrich'
    WHEN ad_name ~* 'whitelisting' OR ad_name ~* 'spark' THEN 'Influencer'
    ELSE 'Superbolt'
END as ad_concept,
ad_name,
ad_id,
ad_status,
visual,
date,
date_granularity,
cost as spend,
impressions,
clicks,
add_billing_events+search_events as purchases,
add_billing_value+page_search_value as revenue,
initiate_checkout_events+product_details_page_browse_events as leads
FROM {{ ref('tiktok_performance_by_ad') }}
