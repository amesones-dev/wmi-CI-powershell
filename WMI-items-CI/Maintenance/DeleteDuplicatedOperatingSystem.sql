WITH DUPLICATES AS
(
	SELECT *,ROW_NUMBER() OVER (PARTITION BY ["__SERVER"],["Caption"],["Version"],["OSArchitecture"]  ORDER BY ["__Server"] ) AS TOTAL
	FROM [dbo].[OperatingSystem]
)


DELETE FROM DUPLICATES WHERE TOTAL > 1

