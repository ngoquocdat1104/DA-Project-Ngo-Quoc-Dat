

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT format_date("%Y%m", parse_date("%Y%m%d", date)) as month
  , SUM(totals.visits) as visits
  , SUM(totals.pageviews) AS pageviews
  , SUM(totals.transactions) AS transactions
  , SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170101' and '20170331'
GROUP BY month      --group by 1
ORDER BY month ASC  --order by 1

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT trafficSource.source as `source`
  , sum(totals.visits) as total_visits
  , sum(totals.bounces) as total_bounce
  , round(sum(totals.bounces)*100.0/sum(totals.visits),2) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where _table_suffix between '01' and '31'
group by `source`
order by total_visits desc

-------------------------------------------------------------------------------------------------------
-- Query 3: Revenue by traffic source by week, by month in June 2017

with formatting as (
    SELECT 
      extract(year from parse_date('%Y%m%d',date)) as year
      , extract(week from parse_date('%Y%m%d',date)) as week
      , left(date,6) as year_month
      , trafficSource.source as source
      , totals.totalTransactionRevenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    where _table_suffix between '01' and '31'
)
select case when week is not null then 'Week' end as time_type 
-- bởi vì ta đã xác định từ đầu sẽ xử lý week và month riêng biệt nên cột time_type e sử dụng case when để trả giá trị mình muốn luôn
  , concat(year, week) as time
  , source
  , sum(`totalTransactionRevenue`)/1000000 as revenue
from formatting
group by time_type, time, source

union all

select case when year_month is not null then 'Month' end as time_type
  , year_month as time
  , source
  , sum(`totalTransactionRevenue`)/1000000 as revenue
from formatting
group by time_type, time, source
order by revenue desc

--cách 2
with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data

-------------------------------------------------------------------------------------------------------
--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with purchased as ( -- tìm tổng pageviews của từng user có purchase theo từng tháng
  
  SELECT left(date,6) as month  --format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    , fullVisitorId AS users
    , SUM(totals.pageviews) AS pagesviews_per_user
  FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
    AND totals.transactions >=1
  GROUP BY month, users 

), non_purchased as ( -- tìm tổng pageviews của từng user ko có purchase theo từng tháng
  
  SELECT left(date,6) as month
    , fullVisitorId AS users
    , SUM(totals.pageviews) AS pagesviews_per_user
  FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
    AND totals.transactions is null
  GROUP BY month, users

), avg_purchased as ( -- tính trung bình theo tháng cho các user có purchase
  
  select month 
    , sum(pagesviews_per_user)/count(users) as avg_pageviews_purchase --count(distinct users)
  from purchased
  group by month

), avg_non_purchased as ( -- tính trung bình theo tháng cho các user k có purchase
  
  select month 
    , sum(pagesviews_per_user)/count(users) as avg_pageviews_non_purchase --count(distinct users)
  from non_purchased
  group by month

)
select avg_non_purchased.month
  , avg_pageviews_purchase
  , avg_pageviews_non_purchase
from avg_purchased 
  join avg_non_purchased
    on avg_purchased.month = avg_non_purchased.month
order by avg_non_purchased.month

-------------------------------------------------------------------------------------------------------
-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

select total.Month
  , avg(total_transactions_per_user) as avg_total_transactions_per_user
from (
  
  --subquery này có tác dụng tính tổng transactions và group theo từng user. Nếu query trực tiếp mà k có bước này thì avg sẽ bị bé đi bởi số record lớn hơn nhiều do chưa được tính tổng theo từng user
  
  select left(date, 6) as Month
    , fullVisitorId as user
    , sum(totals.transactions) as total_transactions_per_user 
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0701' and '0731'
    and totals.transactions >= 1
  group by Month, user

) as total
 group by Month

 --
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions>=1
group by month

-------------------------------------------------------------------------------------------------------
-- Query 06: Average amount of money spent per session
#standardSQL

with total as (
  select left(date, 6) as month
    , fullVisitorId
    , sum(totals.visits) as total_visits_pu
    , sum(totals.totalTransactionRevenue) as total_revenue_pu
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0701' and '0731'
    and totals.visits > 0
    and totals.transactions is not null
  group by month, fullVisitorId
)
select total.month
  , round(sum(total_revenue_pu) / sum(total_visits_pu),2) as Avg_total_transactions_per_user
from total
group by month

--
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(totals.totalTransactionRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions is not null
group by month

-------------------------------------------------------------------------------------------------------
-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

select product.v2ProductName as other_purchased_products
  , sum(product.productQuantity) as quantity 
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  , unnest(hits) as hits
  , unnest(hits.product) as product
where _table_suffix between '0701' and '0731'
  and product.productRevenue is not null
  and product.v2ProductName <> "YouTube Men's Vintage Henley"   
  and fullVisitorId in ( -- câu subquery trả về các user đã từng mua sản phẩm này trog tháng 7, với đk là productrevenue ko được null mới có nghĩa là họ đã mua hàng
                        select fullVisitorId
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
                          , unnest(hits) as hits
                          , unnest(hits.product) as product
                        where _table_suffix between '0701' and '0731'
                          and product.v2ProductName = "YouTube Men's Vintage Henley"
                          and product.productRevenue is not null
                        )
group by product.v2ProductName
order by quantity desc


-------------------------------------------------------------------------------------------------------
--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with sizing as (
  select left(date, 6) as month 
    , count(case when hits.eCommerceAction.action_type = "2" then product.v2ProductName end) as num_product_view
    , count(case when hits.eCommerceAction.action_type = "3" then product.v2ProductName end) as num_addtocart
    , count(case when hits.eCommerceAction.action_type = "6" then product.v2ProductName end)as num_purchase
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    , unnest(hits) as hits
    , unnest(hits.product) as product
  where _table_suffix between '0101' and '0331'
  group by month
  order by month
)
select sizing.month
  , num_product_view
  , num_addtocart
  , num_purchase
  , round(num_addtocart*100.0/num_product_view, 2) as add_to_cart_rate
  , round(num_purchase*100.0/num_product_view, 2) as purchase_rate
from sizing

                                                            
