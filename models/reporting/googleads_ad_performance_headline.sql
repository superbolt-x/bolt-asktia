{{ config (
    alias = target.database + '_googleads_ad_performance_headline'
)}}

{%- set headline_ids = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15"]    -%}
{%- set description_ids = ["1","2","3","4"]    -%}
  
WITH cleaned_ad_data as
    (SELECT date, customer_id, ad_group_id, campaign_id, ad_id, campaign_name, ad_group_name, ad_strength, status,
      {% for headline_id in headline_ids %}
        CASE
            WHEN SPLIT_PART(SPLIT_PART(responsive_search_ad_headlines,'","assetPerformanceLabel"',{{ headline_id }}|int),'{"text":"',2) ~* '\u0027' 
                THEN REPLACE(SPLIT_PART(SPLIT_PART(responsive_search_ad_headlines,'","assetPerformanceLabel"',{{ headline_id }}|int),'{"text":"',2),'\\u0027',CHR(39))
            WHEN SPLIT_PART(SPLIT_PART(responsive_search_ad_headlines,'","assetPerformanceLabel"',{{ headline_id }}|int),'{"text":"',2) ~* '\u0026' 
                THEN REPLACE(SPLIT_PART(SPLIT_PART(responsive_search_ad_headlines,'","assetPerformanceLabel"',{{ headline_id }}|int),'{"text":"',2),'\\u0026',CHR(38))
            ELSE SPLIT_PART(SPLIT_PART(responsive_search_ad_headlines,'","assetPerformanceLabel"',{{ headline_id }}|int),'{"text":"',2)
        END as {{ adapter.quote('headline_'~{{ headline_id }}) }},
      {% endfor %}
      {% for description_id in description_ids %}
        CASE
            WHEN SPLIT_PART(SPLIT_PART(responsive_search_ad_descriptions,'","assetPerformanceLabel"',{{ description_id }}|int),'{"text":"',2) ~* '\u0027' 
                THEN REPLACE(SPLIT_PART(SPLIT_PART(responsive_search_ad_descriptions,'","assetPerformanceLabel"',{{ description_id }}|int),'{"text":"',2),'\\u0027',CHR(39))
            WHEN SPLIT_PART(SPLIT_PART(responsive_search_ad_descriptions,'","assetPerformanceLabel"',{{ description_id }}|int),'{"text":"',2) ~* '\u0026' 
                THEN REPLACE(SPLIT_PART(SPLIT_PART(responsive_search_ad_descriptions,'","assetPerformanceLabel"',{{ description_id }}|int),'{"text":"',2),'\\u0026',CHR(38))
            ELSE SPLIT_PART(SPLIT_PART(responsive_search_ad_descriptions,'","assetPerformanceLabel"',{{ description_id }}|int),'{"text":"',2)
        END as {{ adapter.quote('description_'~{{ description_id }}) }},
      {% endfor %}
        ad_final_urls,
        cost_micros/1000000 as spend,
        clicks,
        impressions
    FROM {{ source ('googleads_raw','ad_performance_report') }})

SELECT
date,
ad_id,
campaign_name, 
ad_group_name,
ad_strength,
{% for {{ headline_id }} in {{ headline_id }}s %}
{{ adapter.quote('headline_'~{{ headline_id }}) }},
{% endfor %}
{% for {{ description_id }} in {{ description_id }}s %}
{{ adapter.quote('description_'~{{ description_id }}) }},
{% endfor %}
ad_final_urls,
COALESCE(SUM(spend),0) as spend,
COALESCE(SUM(impressions),0) as impressions,
COALESCE(SUM(clicks),0) as clicks,
COALESCE(SUM(raspberry),0) as purchases,
COALESCE(SUM(pink),0) as leads
FROM cleaned_ad_data
LEFT JOIN
    (SELECT date, customer_id, ad_id, ad_group_id, campaign_id,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Pink' THEN all_conversions END),0) as pink,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Google Adwords - Raspberry' THEN all_conversions END),0) as raspberry
    FROM {{ source ('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5)
    USING(date, customer_id, ad_id, ad_group_id, campaign_id)
GROUP BY
date,
ad_id,
campaign_name, 
ad_group_name,
ad_strength,
{% for {{ headline_id }} in {{ headline_id }}s %}
{{ adapter.quote('headline_'~{{ headline_id }}) }},
{% endfor %}
{% for {{ description_id }} in {{ description_id }}s %}
{{ adapter.quote('description_'~{{ description_id }}) }},
{% endfor %}
ad_final_urls
