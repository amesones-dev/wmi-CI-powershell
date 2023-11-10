USE [itemsCI]
GO

DELETE FROM [dbo].[NetworkAdapterConfiguration]
      WHERE [itemsCI].[dbo].[NetworkAdapterConfiguration].["IPAddress"] = 'N/A'
GO


