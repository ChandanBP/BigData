reviewData = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id:chararray,user_id:chararray,business_id:chararray,stars:double);
businessData = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id:chararray,full_address:chararray,categories:chararray);

filterData = FILTER businessData by full_address MATCHES '.*CA.*'; 
removeDuplicate = DISTINCT filterData;

groupByBusiness = group reviewData by business_id;
avgData = foreach groupByBusiness generate group as business_id,AVG(reviewData.stars) as avg_rating;
joinBusRevData = JOIN removeDuplicate by business_id,avgData by business_id;
sortedData = order joinBusRevData by avg_rating DESC;
toptenData = limit sortedData 10;
displayData = foreach toptenData generate removeDuplicate::business_id,removeDuplicate::full_address,removeDuplicate::categories,avgData::avg_rating;
dump displayData;
