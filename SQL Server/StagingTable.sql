/****** Object:  Table [dbo].[Staging]    Script Date: 6/1/2025 12:44:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Staging](
	[Date] [date] NOT NULL,
	[Hotel] [varchar](50) NULL,
	[Rooms] [int] NULL,
	[RoomsSold] [int] NULL,
	[RoomsRevenue] [int] NULL,
	[CompSetRooms] [int] NULL,
	[CompSetRoomsSold] [int] NULL,
	[CompSetRoomsRevenue] [int] NULL
) ON [PRIMARY]
GO


