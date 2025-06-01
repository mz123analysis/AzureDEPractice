/****** Object:  Table [dbo].[ProdTable]    Script Date: 6/1/2025 12:43:42 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ProdTable](
	[Date] [date] NULL,
	[Hotel] [varchar](50) NULL,
	[Rooms] [int] NULL,
	[RoomsSold] [int] NULL,
	[RoomsRevenue] [int] NULL,
	[CompSetRooms] [int] NULL,
	[CompSetRoomsSold] [int] NULL,
	[CompSetRoomsRevenue] [int] NULL,
	[Occ] [float] NULL,
	[ADR] [float] NULL,
	[RevPAR] [float] NULL,
	[CompSetOcc] [float] NULL,
	[CompSetADR] [float] NULL,
	[CompSetRevPAR] [float] NULL,
	[OCI] [float] NULL,
	[API] [float] NULL,
	[RPI] [float] NULL
) ON [PRIMARY]
GO


