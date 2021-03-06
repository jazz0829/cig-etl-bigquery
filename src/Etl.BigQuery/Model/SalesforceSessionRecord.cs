using System;

namespace Eol.Cig.Etl.BigQuery.Model
{
    public static class SalesforceSessionRecord
    {
        public static string Query(string dataSetId, DateTime date) => $@"
            SELECT
              visitorId,
              visitNumber,
              visitId,
              visitStartTime,
              date,
              totals.visits AS totals_visits,
              totals.hits AS totals_hits,
              totals.pageviews AS totals_pageviews,
              totals.timeOnSite AS totals_timeOnSite,
              totals.bounces AS totals_bounces,
              totals.transactions AS totals_transactions,
              totals.transactionRevenue AS totals_transactionRevenue,
              totals.newVisits AS totals_newVisits,
              totals.screenviews AS totals_screenViews,
              totals.uniqueScreenviews AS totals_uniqueScreenViews,
              totals.timeOnScreen AS totals_timeOnScreen,
              totals.totalTransactionRevenue AS totals_totalTransactionRevenue,
              --totals.sessionQualityDim AS totals_sessionQualityDim,
              trafficSource.referralPath AS trafficSource_referralPath,
              trafficSource.campaign AS trafficSource_campaign,
              trafficSource.source AS trafficSource_source,
              trafficSource.medium AS trafficSource_medium,
              trafficSource.keyword AS trafficSource_keyword,
              trafficSource.adContent AS trafficSource_adContent,
              trafficSource.adwordsClickInfo.campaignId AS trafficSource_adwordsClickInfo_campaignId,
              trafficSource.adwordsClickInfo.adGroupId AS trafficSource_adwordsClickInfo_adGroupId,
              trafficSource.adwordsClickInfo.creativeId AS trafficSource_adwordsClickInfo_creativeId,
              trafficSource.adwordsClickInfo.criteriaId AS trafficSource_adwordsClickInfo_criteriaId,
              trafficSource.adwordsClickInfo.page AS trafficSource_adwordsClickInfo_page,
              trafficSource.adwordsClickInfo.slot AS trafficSource_adwordsClickInfo_slot,
              trafficSource.adwordsClickInfo.criteriaParameters AS trafficSource_adwordsClickInfo_criteriaParameters,
              trafficSource.adwordsClickInfo.gclId AS trafficSource_adwordsClickInfo_gclId,
              trafficSource.adwordsClickInfo.customerId AS trafficSource_adwordsClickInfo_customerId,
              trafficSource.adwordsClickInfo.adNetworkType AS trafficSource_adwordsClickInfo_adNetworkType,
              trafficSource.adwordsClickInfo.targetingCriteria.boomUserlistId AS trafficSource_adwordsClickInfo_targetCriteria_boomUserlistId,
              trafficSource.adwordsClickInfo.isVideoAd AS trafficSource_adwordsClickInfo_isVideAd,
              trafficSource.isTrueDirect AS trafficSource_isTrueDirect,
              trafficSource.campaignCode AS trafficSource_campaignCode,
              device.browser AS device_browser,
              device.browserVersion AS device_browserVersion,
              device.browserSize AS device_browserSize,
              device.operatingSystem AS device_operatingSystem,
              device.operatingSystemVersion AS device_operatingSystemVersion,
              device.isMobile AS device_isMobile,
              device.mobileDeviceBranding AS device_mobileDeviceBranding,
              device.mobileDeviceModel AS device_mobileDeviceModel,
              device.mobileInputSelector AS device_mobileInputSelector,
              device.mobileDeviceInfo AS device_mobileDeviceInfo,
              device.mobileDeviceMarketingName AS device_mobileDeviceMarketingName,
              device.flashVersion AS device_flashVersion,
              device.javaEnabled AS device_javaEnabled,
              device.LANGUAGE AS device_language,
              device.screenColors AS device_screenColors,
              device.screenResolution AS device_screenResolution,
              device.deviceCategory AS device_deviceCategory,
              geoNetwork.continent AS geoNetwork_continent,
              geoNetwork.subContinent AS geoNetwork_subContinent,
              geoNetwork.country AS geoNetwork_country,
              geoNetwork.region AS geoNetwork_region,
              cd.index AS CustomDimensions_index,
              cd.value AS CustomDimensions_value,
              h.hitNumber AS hits_hitNumber,
              h.time AS hits_time,
              h.hour AS hits_hour,
              h.minute AS hits_minute,
              h.isSecure AS hits_isSecure,
              h.isInteraction AS hits_isInteraction,
              h.isEntrance AS hits_isEntrance,
              h.isExit AS hits_isExit,
              h.referer AS hits_referer,
              h.page.pagePath AS hits_page_pagePath,
              h.page.hostname AS hits_page_hostname,
              h.page.pageTitle AS hits_page_pageTitle,
              h.page.searchKeyword AS hits_page_searchKeyword,
              h.page.searchCategory AS hits_page_searchCategory,
              h.page.pagePathLevel1 AS hits_page_pagePathLevel1,
              h.page.pagePathLevel2 AS hits_page_pagePathLevel2,
              h.page.pagePathLevel3 AS hits_page_pagePathLevel3,
              h.page.pagePathLevel4 AS hits_page_pagePathLevel4,
              h.transaction.transactionId AS hits_transaction_transactionId,
              h.transaction.transactionRevenue AS hits_transaction_transactionRevenue,
              h.transaction.transactionTax AS hits_transaction_transactionTax,
              h.transaction.transactionShipping AS hits_transaction_transactionShipping,
              h.transaction.affiliation AS hits_transaction_affiliation,
              h.transaction.currencyCode AS hits_transaction_currencyCode,
              h.transaction.localTransactionRevenue AS hits_transaction_localTransactionRevenue,
              h.transaction.localTransactionTax AS hits_transaction_localTransactionTax,
              h.transaction.localTransactionShipping AS hits_transaction_localTransactionShipping,
              h.transaction.transactionCoupon AS hits_transaction_transactionCoupon,
              h.item.transactionId AS hits_item_transactionId,
              h.item.productName AS hits_item_productName,
              h.item.productCategory AS hits_item_productCategory,
              h.item.productSku AS hits_item_productSku,
              h.item.itemQuantity AS hits_item_itemQuantity,
              h.item.itemRevenue AS hits_item_itemRevenue,
              h.item.currencyCode AS hits_item_currencyCode,
              h.item.localItemRevenue AS hits_item_localItemRevenue,
              h.contentInfo.contentDescription AS hits_contentInfo_contentDescription,
              h.appInfo.name AS hits_appInfo_name,
              h.appInfo.version AS hits_appInfo_version,
              h.appInfo.id AS hits_appInfo_id,
              h.appInfo.installerId AS hits_appInfo_installerId,
              h.appInfo.appInstallerId AS hits_appInfo_appInstallerId,
              h.appInfo.appName AS hits_appInfo_appName,
              h.appInfo.appVersion AS hits_appInfo_appVersion,
              h.appInfo.appId AS hits_appInfo_appId,
              h.appInfo.screenName AS hits_appInfo_screenName,
              h.appInfo.landingScreenName AS hits_appInfo_landingScreenName,
              h.appInfo.exitScreenName AS hits_appInfo_exitScreenName,
              h.appInfo.screenDepth AS hits_appInfo_sceenDepth,
              h.exceptionInfo.description AS hits_exceptionInfo_description,
              h.exceptionInfo.isFatal AS hits_exceptionInfo_isFatal,
              h.exceptionInfo.exceptions AS hits_exceptionInfo_exceptions,
              h.exceptionInfo.fatalExceptions AS hits_exceptionInfo_fatalExceptions,
              h.eventInfo.eventCategory AS hits_eventInfo_eventCategory,
              h.eventInfo.eventAction AS hits_eventInfo_eventAction,
              h.eventInfo.eventLabel AS hits_eventInfo_eventLabel,
              h.eventInfo.eventValue AS hits_eventInfo_eventValue,
              hp.productSKU AS hits_product_productSKU,
              hp.v2ProductName AS hits_product_v2ProductName,
              hp.v2ProductCategory AS hits_product_v2ProductCategory,
              hp.productVariant AS hits_product_productVariant,
              hp.productBrand AS hits_product_productBrand,
              hp.productRevenue AS hits_product_productRevenue,
              hp.localProductRevenue AS hits_product_localProductRevenue,
              hp.productPrice AS hits_product_productPrice,
              hp.localProductPrice AS hits_product_localProductPrice,
              hp.productQuantity AS hits_product_productQuantity,
              hp.productRefundAmount AS hits_product_productRefundAmount,
              hp.localProductRefundAmount AS hits_product_localProductRefundAmount,
              hp.isImpression AS hits_product_isImpression,
              hp.isClick AS hits_product_isClick,
              hpcd.index AS hits_product_customDimensions_index,
              hpcd.value AS hits_product_customDimensions_value,
              hpcm.index AS hits_product_customMetrics_index,
              hpcm.value AS hits_product_customMetrics_value,
              hp.productListName AS hits_product_productListName,
              hp.productListPosition AS hits_product_productListPosition,
              hpromo.promoId AS hits_promotion_promoId,
              hpromo.promoName AS hits_promotion_promoName,
              hpromo.promoCreative AS hits_promotion_promoCreative,
              hpromo.promoPosition AS hits_promotion_promoPosition,
              h.promotionActionInfo.promoIsView AS hits_prmotionActionInfo_promoIsView,
              h.promotionActionInfo.promoIsClick AS hits_prmotionActionInfo_promoIsClick,
              h.refund.refundAmount AS hits_refund_refoundAmount,
              h.refund.localRefundAmount AS hits_refund_localRefundAmount,
              h.eCommerceAction.action_type AS hits_eCommerceAction_action_type,
              h.eCommerceAction.step AS hits_eCommerceAction_step,
              h.eCommerceAction.option AS hits_eCommerceAction_option,
              he.experimentId AS hits_experiment_experimentId,
              he.experimentVariant AS hits_experiment_experimentVariant,
              h.publisher.dfpClicks AS hits_publisher_dfpClicks,
              h.publisher.dfpImpressions AS hits_publisher_dfpImpressions,
              h.publisher.dfpMatchedQueries AS hits_publisher_dfpMatchedQueries,
              h.publisher.dfpMeasurableImpressions AS hits_publisher_dfpMeasurableImpressions,
              h.publisher.dfpQueries AS hits_publisher_dfpQueries,
              h.publisher.dfpRevenueCpm AS hits_publisher_dfpRevenueCpm,
              h.publisher.dfpRevenueCpc AS hits_publisher_dfpRevenueCpc,
              h.publisher.dfpViewableImpressions AS hits_publisher_dfpViewableImpressions,
              h.publisher.dfpPagesViewed AS hits_publisher_dfpPagesViewed,
              h.publisher.adsenseBackfillDfpClicks AS hits_publisher_adsenseBackfillDfpClicks,
              h.publisher.adsenseBackfillDfpImpressions AS hits_publisher_adsenseBackfillDfpImpressions,
              h.publisher.adsenseBackfillDfpMatchedQueries AS hits_publisher_adsenseBackfillDfpMatchedQueries,
              h.publisher.adsenseBackfillDfpMeasurableImpressions AS hits_publisher_adsenseBackfillDfpMeasurableImpressions,
              h.publisher.adsenseBackfillDfpQueries AS hits_publisher_adsenseBackfillDfpQueries,
              h.publisher.adsenseBackfillDfpRevenueCpm AS hits_publisher_adsenseBackfillDfpRevenueCpm,
              h.publisher.adsenseBackfillDfpRevenueCpc AS hits_publisher_adsenseBackfillDfpRevenueCpc,
              h.publisher.adsenseBackfillDfpViewableImpressions AS hits_publisher_adsenseBackfillDfpViewableImpressions,
              h.publisher.adsenseBackfillDfpPagesViewed AS hits_publisher_adsenseBackfillDfpPagesViewed,
              h.publisher.adxBackfillDfpClicks AS hits_publisher_adxBackfillDfpClicks,
              h.publisher.adxBackfillDfpImpressions AS hits_publisher_adxBackfillDfpImpressions,
              h.publisher.adxBackfillDfpMatchedQueries AS hits_publisher_adxBackfillDfpMatchedQueries,
              h.publisher.adxBackfillDfpMeasurableImpressions AS hits_publisher_adxBackfillDfpMeasurableImpressions,
              h.publisher.adxBackfillDfpQueries AS hits_publisher_adxBackfillDfpQueries,
              h.publisher.adxBackfillDfpRevenueCpm AS hits_publisher_adxBackfillDfpRevenueCpm,
              h.publisher.adxBackfillDfpRevenueCpc AS hits_publisher_adxBackfillDfpRevenueCpc,
              h.publisher.adxBackfillDfpViewableImpressions AS hits_publisher_adxBackfillDfpViewableImpressions,
              h.publisher.adxBackfillDfpPagesViewed AS hits_publisher_adxBackfillDfpPagesViewed,
              h.publisher.adxClicks AS hits_publisher_adxClicks,
              h.publisher.adxImpressions AS hits_publisher_adxImpressions,
              h.publisher.adxMatchedQueries AS hits_publisher_adxMatchedQueries,
              h.publisher.adxMeasurableImpressions AS hits_publisher_adxMeasurableImpressions,
              h.publisher.adxQueries AS hits_publisher_adxQueries,
              h.publisher.adxRevenue AS hits_publisher_adxRevenue,
              h.publisher.adxViewableImpressions AS hits_publisher_adxViewableImpressions,
              h.publisher.adxPagesViewed AS hits_publisher_adxPagesViewed,
              h.publisher.adsViewed AS hits_publisher_adsViewed,
              h.publisher.adsUnitsViewed AS hits_publisher_adsUnitsViewed,
              h.publisher.adsUnitsMatched AS hits_publisher_adsUnitsMatched,
              h.publisher.viewableAdsViewed AS hits_publisher_viewableAdsViewed,
              h.publisher.measurableAdsViewed AS hits_publisher_measurableAdsViewed,
              h.publisher.adsPagesViewed AS hits_publisher_adsPagesViewed,
              h.publisher.adsClicked AS hits_publisher_adsClicked,
              h.publisher.adsRevenue AS hits_publisher_adsRevenue,
              h.publisher.dfpAdGroup AS hits_publisher_dfpAdGroup,
              h.publisher.dfpAdUnits AS hits_publisher_dfpAdUnits,
              h.publisher.dfpNetworkId AS hits_publisher_dfpNetworkId,
              hcv.index AS hits_custom_variables_index,
              hcv.customVarName AS hits_custom_variables_customVarName,
              hcv.customVarValue AS hits_custom_variables_ustomVarValue,
              hcd.index AS hits_customDimensions_index,
              hcd.value AS hits_customDimensions_value,
              hcm.index AS hits_customMetrics_index,
              hcm.value AS hits_customMetrics_value,
              h.type AS hits_type,
              h.social.socialInteractionNetwork AS hits_social_socialInteractionNetwork,
              h.social.socialInteractionAction AS hits_social_socialInteractionAction,
              h.social.socialInteractions AS hits_social_socialInteractions,
              h.social.socialInteractionTarget AS hits_social_socialInteractionTarget,
              h.social.socialNetwork AS hits_social_socialNetwork,
              h.social.uniqueSocialInteractions AS hits_social_uniqueSocialInteractions,
              h.social.hasSocialSourceReferral AS hits_social_hasSocialSourceReferral,
              h.social.socialInteractionNetworkAction AS hits_social_socialInteractionNetworkAction,
              h.latencyTracking.pageLoadSample AS hits_latencyTracking_pageLoadSample,
              h.latencyTracking.pageLoadTime AS hits_latencyTracking_pageLoadTime,
              h.latencyTracking.pageDownloadTime AS hits_latencyTracking_pageDownloadTime,
              h.latencyTracking.redirectionTime AS hits_latencyTracking_redirectionTime,
              h.latencyTracking.speedMetricsSample AS hits_latencyTracking_speedMetricsSample,
              h.latencyTracking.domainLookupTime AS hits_latencyTracking_domainLookupTime,
              h.latencyTracking.serverConnectionTime AS hits_latencyTracking_serverConnectionTime,
              h.latencyTracking.serverResponseTime AS hits_latencyTracking_serverResponseTime,
              h.latencyTracking.domLatencyMetricsSample AS hits_latencyTracking_domLatencyMetricsSample,
              h.latencyTracking.domInteractiveTime AS hits_latencyTracking_domInteractiveTime,
              h.latencyTracking.domContentLoadedTime AS hits_latencyTracking_domContentLoadedTime,
              h.latencyTracking.userTimingValue AS hits_latencyTracking_userTimingValue,
              h.latencyTracking.userTimingSample AS hits_latencyTracking_userTimingSample,
              h.latencyTracking.userTimingVariable AS hits_latencyTracking_userTimingVariable,
              h.latencyTracking.userTimingCategory AS hits_latencyTracking_userTimingCategory,
              h.latencyTracking.userTimingLabel AS hits_latencyTracking_userTimingLabel,
              h.sourcePropertyInfo.sourcePropertyDisplayName AS hits_sourcePropertyInfo_sourcePropertyDisplayName,
              h.sourcePropertyInfo.sourcePropertyTrackingId AS hits_sourcePropertyInfo_sourcePropertyTrackingId,
              h.contentGroup.contentGroup1 AS hits_contentGroup_contentGroup1,
              h.contentGroup.contentGroup2 AS hits_contentGroup_contentGroup2,
              h.contentGroup.contentGroup3 AS hits_contentGroup_contentGroup3,
              h.contentGroup.contentGroup4 AS hits_contentGroup_contentGroup4,
              h.contentGroup.contentGroup5 AS hits_contentGroup_contentGroup5,
              h.contentGroup.previousContentGroup1 AS hits_contentGroup_previousContentGroup1,
              h.contentGroup.previousContentGroup2 AS hits_contentGroup_previousContentGroup2,
              h.contentGroup.previousContentGroup3 AS hits_contentGroup_previousContentGroup3,
              h.contentGroup.previousContentGroup4 AS hits_contentGroup_previousContentGroup4,
              h.contentGroup.previousContentGroup5 AS hits_contentGroup_previousContentGroup5,
              h.contentGroup.contentGroupUniqueViews1 AS hits_contentGroup_contentGroupUniqueViews1,
              h.contentGroup.contentGroupUniqueViews2 AS hits_contentGroup_contentGroupUniqueViews2,
              h.contentGroup.contentGroupUniqueViews3 AS hits_contentGroup_contentGroupUniqueViews3,
              h.contentGroup.contentGroupUniqueViews4 AS hits_contentGroup_contentGroupUniqueViews4,
              h.contentGroup.contentGroupUniqueViews5 AS hits_contentGroup_contentGroupUniqueViews5,
              h.dataSource AS hits_dataSource,
              fullVisitorId,
              userId,
              channelGrouping,
              socialEngagementType
            FROM
              `ga-360-168411.{dataSetId.Trim()}.ga_sessions_{date.Date.ToString("yyyMMdd")}`
            LEFT JOIN
              UNNEST(customDimensions) AS cd
            LEFT JOIN
              UNNEST(hits) AS h
            LEFT JOIN
              UNNEST(h.product) AS hp
            LEFT JOIN
              UNNEST(hp.customDimensions) AS hpcd
            LEFT JOIN
              UNNEST(hp.customMetrics) AS hpcm
            LEFT JOIN
              UNNEST(h.promotion) AS hpromo
            LEFT JOIN
              UNNEST(h.experiment) AS he
            LEFT JOIN
              UNNEST(h.customVariables) AS hcv
            LEFT JOIN
              UNNEST(h.customDimensions) AS hcd
            LEFT JOIN
              UNNEST(h.customMetrics) AS hcm WHERE h.page.hostname = 'support.exactonline.com'";
    }
}
