select * from procurement.offlinesales where orderid != 'orderid' limit 10;

/* Count of total transactions */

select count(*) from procurement.offlinesales where orderid!='orderid'; 

select count(distinct product) from procurement.offlinesales where orderid!='orderid'; 

select product,avg(quantityordered * unitprice) as avg_sales_amount from procurement.offlinesales where orderid!='orderid' group by product order by avg_sales_amount desc;

/* Anomalous Transactions */
WITH stats AS (
 SELECT product,
        AVG(quantityordered * unitprice)  AS avg_value, 
        STDDEV(quantityordered * unitprice) / AVG(quantityordered * unitprice) AS rsd_value
 FROM procurement.offlinesales
 GROUP BY product)
SELECT orderid, orderdate, product, (quantityordered * unitprice) as sales_amount,
      ABS(1 - (quantityordered * unitprice)/ avg_value) AS distance_from_avg      
FROM procurement.offlinesales INNER JOIN stats USING (product)
WHERE rsd_value <= 0.2 
ORDER BY distance_from_avg DESC
LIMIT 10