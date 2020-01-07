




CREATE PROCEDURE [dbo].[usp_Load_Dim_Team] 
AS

UPDATE Dest
SET
Dest.[TeamMember1_Name]   = Staging.[TeamMember1_Name],
Dest.[TeamMember2_Name]	  = Staging.[TeamMember2_Name],
Dest.[HashKey]			  = Staging.[HashKey],
Dest.[ETLBatchID_Update]  = Staging.[ETLBatchID_Update]
FROM
[DW].[Dim_Team] Dest
INNER JOIN [Updates].[Dim_Team_Update] Staging ON Dest.Team_sk = Staging.Team_sk_Dest