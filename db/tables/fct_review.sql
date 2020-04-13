-- amazon_review.fct_review definition

-- Drop table

-- DROP TABLE amazon_review.fct_review;

CREATE TABLE amazon_review.fct_review (
	customer_id varchar(255) NOT NULL,
	product_id varchar(255) NOT NULL,
	total_votes_sum numeric(10) NULL,
	helpful_votes_sum numeric(10) NULL,
	star_rating_sum numeric(10) NULL,
	star_rating_mean numeric NULL,
	ttl_review_count numeric(10) NULL,
	CONSTRAINT fct_review_pkey PRIMARY KEY (customer_id, product_id)
);