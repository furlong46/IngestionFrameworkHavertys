$Connection = Get-AutomationConnection -Name "AzureRunAsConnection"
$TenantId = $Connection.TenantId
$SubscriptionId = $Connection.SubscriptionId

# Get Azure Credential
$AzureCred = Get-AutomationPSCredential -Name 'DataAnalyticsQAServicePrincipal'

# Login to Azure 
Write-Output "Login to Azure using automation account."
Connect-AzAccount -TenantId $TenantId -SubscriptionId $SubscriptionId -ServicePrincipal -Credential $AzureCred

#Import Modules
Import-Module -Name Az.Sql

#Get password
$ResourceGroupName = Get-AutomationVariable -Name "ResourceGroupName"
$SQLServer = Get-AutomationVariable -Name 'SQLServerName'
$SQLServerDBName = Get-AutomationVariable -Name 'SQLServerDBName' 
$SQLEdition = Get-AutomationVariable -Name 'SQLServerEdition'
[int]$SQLVCore_ScaleDown = Get-AutomationVariable -Name 'SQLServerVCore_ScaleDown'
[int]$SQLVCore_ScaleUp = Get-AutomationVariable -Name 'SQLServerVCore_ScaleUp'

#Check if the DB already exists
$DBExists = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $SQLServer -DatabaseName $SQLServerDBName
 
if ($DBExists.Capacity -ge $SQLVCore_ScaleUp) {

    Write-Output "Scaling Down"   
    $database = Set-AzSqlDatabase -ResourceGroupName $resourceGroupName `
    -ServerName $SQLServer `
    -DatabaseName $SQLServerDBName `
    -Edition $SQLEdition `
    -VCore $SQLVCore_ScaleDown
}
else { 
    Write-Output "Scaling Up"
    $database = Set-AzSqlDatabase -ResourceGroupName $resourceGroupName `
    -ServerName $SQLServer `
    -DatabaseName $SQLServerDBName `
    -Edition $SQLEdition `
    -VCore $SQLVCore_ScaleUp
}