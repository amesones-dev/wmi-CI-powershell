WITH DUPLICATES AS
(
	SELECT *,ROW_NUMBER() OVER (PARTITION BY ["__Server"],["Username"],["Model"],["Manufacturer"]  ORDER BY ["__Server"] ) AS TOTAL
	FROM ComputerSystem
)
DELETE FROM DUPLICATES WHERE TOTAL > 1