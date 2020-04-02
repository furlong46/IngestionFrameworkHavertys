

CREATE FUNCTION [dbo].[udf_GetETLConfiguration]
(
    @ConfigKey varchar(50)
)
RETURNS varchar(500)
AS
BEGIN
    -- Declare the return variable here
    DECLARE @ConfigValue varchar(500) = ISNULL((SELECT ConfigValue FROM [ETL].[Configurations] WHERE COnfigKey=@ConfigKey), '')

    -- Return the result of the function
    RETURN @ConfigValue
END