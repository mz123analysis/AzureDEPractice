/****** Object:  StoredProcedure [dbo].[SP_Staging_to_Prod]    Script Date: 6/1/2025 12:44:47 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:      <Myron Zhang>
-- Create Date: <5/31/25>
-- Description: <Staging to Production>
-- =============================================
CREATE PROCEDURE [dbo].[SP_Staging_to_Prod]
AS
BEGIN

	INSERT INTO [dbo].[ProdTable]
	([Date],[Hotel],[Rooms],[RoomsSold],[RoomsRevenue],[CompSetRooms],[CompSetRoomsSold],[CompSetRoomsRevenue]
      ,[Occ],[ADR],[RevPAR],[CompSetOcc],[CompSetADR],[CompSetRevPAR],[OCI],[API],[RPI])
	  SELECT 
	S.Date, S.Hotel, S.Rooms, S.RoomsSold, S.RoomsRevenue, S.CompSetRooms, S.CompSetRoomsSold, S.CompSetRoomsRevenue,
	  Cast(S.RoomsSold as FLOAT)/CAST(S.Rooms as FLOAT) * 100 as Occ, CAST(S.RoomsRevenue AS FLOAT)/ CAST(S.RoomsSold AS FLOAT) as ADR, CAST(S.RoomsRevenue AS FLOAT)/CAST(S.RoomsSold AS FLOAT) * CAST(S.RoomsSold as FLOAT)/CAST(S.Rooms AS FLOAT) as RevPAR, 
	  (CAST(S.CompSetRoomsSold AS FLOAT)/CAST(S.CompSetRooms AS FLOAT) * 100) as CompSetOcc, (CAST(S.CompSetRoomsRevenue AS FLOAT)/ CAST(S.CompSetRoomsSold AS FLOAT)) as CompSetADR, 
	  CAST(S.CompSetRoomsSold AS FLOAT)/CAST(S.CompSetRooms AS FLOAT) *(CAST(S.CompSetRoomsRevenue AS FLOAT) / CAST(S.CompSetRoomsSold AS FLOAT))  as CSRevPAR, ((Cast(S.RoomsSold as FLOAT)/CAST(S.Rooms as FLOAT))/(CAST(S.CompSetRoomsSold AS FLOAT)/CAST(S.CompSetRooms AS FLOAT))) as OCI,
	  (CAST(S.RoomsRevenue AS FLOAT)/ CAST(S.RoomsSold AS FLOAT)) /((CAST(S.CompSetRoomsRevenue AS FLOAT)/ CAST(S.CompSetRoomsSold AS FLOAT))) as ARI, (CAST(S.RoomsRevenue AS FLOAT)/CAST(S.RoomsSold AS FLOAT) * CAST(S.RoomsSold as FLOAT)/CAST(S.Rooms AS FLOAT))/(CAST(S.CompSetRoomsSold AS FLOAT)/CAST(S.CompSetRooms AS FLOAT) *(CAST(S.CompSetRoomsRevenue AS FLOAT) / CAST(S.CompSetRoomsSold AS FLOAT))) as RPI
	  FROM [dbo].[Staging] as S
	  LEFT JOIN [dbo].[ProdTable] as P on S.Date = P.Date AND S.Hotel = P.Hotel
	  WHERE P.Date iS NULL AND P.Hotel IS NULL
END
GO


