--- Example Extract

select 	bt.id_transaction ,
		bt.id_customer ,
		bc.birthdate_customer::DATE,
		bc.gender_customer ,
		bc.country_customer ,
		bt.date_transaction::DATE,
		bt.product_transaction ,
		bt.amount_transaction
from bigdata_transaction bt 
	left join bigdata_customer bc on bt.id_customer = bc.id_customer;