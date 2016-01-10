reviewData = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id:chararray,user_id:chararray,business_id:chararray,stars:float);
businessData = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id:chararray,full_address:chararray,categories:chararray);

filterData = DISTINCT businessData;

busiReviewGroup = cogroup reviewData by business_id,filterData by business_id; 
limitData = limit busiReviewGroup 5;
dump limitData;
