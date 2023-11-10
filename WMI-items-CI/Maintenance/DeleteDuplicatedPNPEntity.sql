WITH DUPLICATES AS
(
	SELECT *,ROW_NUMBER() OVER (PARTITION BY ["__SERVER"],["Caption"],["Manufacturer"],["PNPClass"]  ORDER BY ["__Server"] ) AS TOTAL
	FROM [itemsCI].[dbo].[PnPEntity]
)


DELETE FROM DUPLICATES WHERE TOTAL > 1



