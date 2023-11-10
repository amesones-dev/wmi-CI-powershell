WITH DUPLICATES AS
(
	SELECT *,ROW_NUMBER() OVER (PARTITION BY ["__SERVER"],["MACAddress"],["Description"],["IPAddress"],["ServiceName"]  ORDER BY ["__Server"] ) AS TOTAL
	FROM NetworkAdapterConfiguration
)


DELETE FROM DUPLICATES WHERE TOTAL > 1

