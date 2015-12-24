reviewData = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id:chararray,user_id:chararray,business_id:chararray,stars:double);
businessData = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id:chararray,full_address:chararray,categories:chararray);

filterData = FILTER businessData by not(full_address MATCHES '.*CA.*');
removeDuplicate = DISTINCT filterData;

groupByBusiness = group reviewData by business_id;
countBusiness = FOREACH groupByBusiness GENERATE group as business_id, COUNT(reviewData.stars) as numReviews;
joinBusRevData = JOIN removeDuplicate by business_id,countBusiness by business_id;
finalData = FILTER joinBusRevData by full_address is not null and categories is not null;
maxBusiness = order finalData by numReviews DESC;

toptenData = limit maxBusiness 10;
removeColumnDuplicate = foreach toptenData generate removeDuplicate::business_id,removeDuplicate::full_address,removeDuplicate::categories,countBusiness::numReviews;
dump removeColumnDuplicate;
STORE removeColumnDuplicate INTO '/cbp140230/HW3/Q2' using PigStorage('^');