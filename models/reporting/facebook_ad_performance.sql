{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
CASE
    WHEN (campaign_name ~* '- la -' AND campaign_name !~* 'retargeting') OR (adset_name ~* 'la -' AND adset_name !~* 'RET') THEN 'Prospecting LA'
    WHEN (campaign_name ~* '- ny -' AND campaign_name !~* 'retargeting') OR (adset_name ~* 'ny -' AND adset_name !~* 'RET') THEN 'Prospecting NY'
    WHEN (campaign_name ~* '- phx -' OR campaign_name ~* '- az -') AND campaign_name !~* 'retargeting' THEN 'Prospecting PHX'
    WHEN campaign_name ~* '- sf -' AND campaign_name !~* 'retargeting' THEN 'Prospecting SF'
    WHEN campaign_name ~* 'retargeting - la -' OR (adset_name ~* 'la -' AND adset_name ~* 'RET') THEN 'Retargeting LA'
    WHEN campaign_name ~* 'retargeting - ny -' OR (adset_name ~* 'ny -' AND adset_name ~* 'RET') THEN 'Retargeting NY'
    WHEN (campaign_name ~* 'retargeting - phx -' OR campaign_name ~* 'retargeting - az -') THEN 'Retargeting PHX'
    WHEN campaign_name ~* 'retargeting - sf -' THEN 'Retargeting SF'
    WHEN campaign_name = '[SGS] Prospecting - NY - Front Door Test - Purchases' THEN 'Front Door'
    ELSE 'Other'
END as campaign_type_custom,
CASE
    WHEN (campaign_name ~* '- la -' OR campaign_name ~* '- ny -' OR campaign_name ~* '- phx -' OR campaign_name ~* '- az -' OR campaign_name ~* '- sf -') THEN 'Market Specific'
    WHEN (campaign_name ~* 'all-markets' AND campaign_name ~* 'advantage+') THEN 'Adv+ Market Consolidated'
    WHEN campaign_name ~* 'all-markets - evergreen'  THEN 'New Market Consolidated'
    ELSE 'Other'
END as campaign_type_market,
CASE WHEN campaign_name ~* '- la -' /*OR adset_name ~* 'la -'*/ THEN 'LA'
    WHEN campaign_name ~* '- ny -' OR adset_name ~* 'ny -' THEN 'NY'
    WHEN campaign_name ~* '- phx -' OR campaign_name ~* '- az -' THEN 'PHX'
    WHEN campaign_name ~* '- sf -' THEN 'SF'
    ELSE 'Other'
END as location,
adset_name,
adset_id,
adset_effective_status,
audience,
CASE
    WHEN ad_name ~* 'Ostrich' THEN 'Ostrich'
    WHEN ad_name ~* 'whitelisting' OR ad_name ~* 'spark' THEN 'Influencer'
    ELSE 'Superbolt'
END as ad_concept,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
"offsite_conversion.fb_pixel_custom.raspberry" as purchases,
"offsite_conversion.fb_pixel_custom.pink" as leads,
"offsite_conversion.fb_pixel_custom.pistachio" as pistachio,
"offsite_conversion.fb_pixel_custom.poppy" as poppy,
"offsite_conversion.fb_pixel_custom.oat" as oat,
"offsite_conversion.fb_pixel_custom.gold" as gold,
"offsite_conversion.fb_pixel_custom.clay" as clay,
onfacebook_leads as onplatform_leads,
"offsite_conversion.custom.264161598804766" as typeform_submit,
"offsite_conversion.custom.1216044982114674" as email_signup,
"offsite_conversion.custom.485796842084919" as cervical_cancer_visit,
"offsite_conversion.custom.291239861951635" as text_message
FROM {{ ref('facebook_performance_by_ad') }}
