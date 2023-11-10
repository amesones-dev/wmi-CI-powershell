WITH DUPLICATES AS
(
	SELECT *,ROW_NUMBER() OVER (PARTITION BY ["__SERVER"],["Caption"],["Vendor"],["Version"]  ORDER BY ["__Server"] ) AS TOTAL
	FROM itemsCI.dbo.Product
)


DELETE FROM DUPLICATES WHERE TOTAL > 1



