reviewData = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id:chararray,user_id:chararray,business_id:chararray,stars:float);
businessData = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id:chararray,full_address:chararray,categories:chararray);

filterData = FILTER businessData by not(full_address MATCHES '.*CA.*');
removeDuplicate = DISTINCT filterData;

busiReviewGroup = cogroup reviewData by business_id INNER,removeDuplicate by business_id INNER;
countReviews = foreach busiReviewGroup generate group as business_id,flatten(removeDuplicate),COUNT(reviewData.stars) as numReviews;
sortedData = order countReviews by numReviews DESC;
limitData = limit sortedData 5;
displayData = foreach limitData generate business_id,removeDuplicate::full_address,removeDuplicate::categories,numReviews;
dump displayData;