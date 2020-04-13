-- amazon_review.dim_product definition

-- Drop table

-- DROP TABLE amazon_review.dim_product;

CREATE TABLE amazon_review.dim_product (
	product_id varchar(255) NOT NULL,
	product_category varchar(255) NULL,
	product_title varchar(10000) NULL,
	CONSTRAINT dim_product_pkey PRIMARY KEY (product_id)
);