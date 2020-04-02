$Connection = Get-AutomationConnection -Name "AzureRunAsConnection"
$TenantId = $Connection.TenantId
$SubscriptionId = $Connection.SubscriptionId

# Get Azure Credential
$AzureCred = Get-AutomationPSCredential -Name 'DataAnalyticsQAServicePrincipal'

# Login to Azure 
Write-Output "Login to Azure using automation account."
Connect-AzAccount -TenantId $TenantId -SubscriptionId $SubscriptionId -ServicePrincipal -Credential $AzureCred

#Import Modules
Import-Module -Name Az.AnalysisServices

#Get password
$ResourceGroupName = Get-AutomationVariable -Name "ResourceGroupName"
$AnalysisServerName = Get-AutomationVariable -Name 'AnalysisServerName'
$AnalysisServerNameFull = Get-AutomationVariable -Name 'AnalysisServerNameFull'

# Get old status (for testing/logging purpose only)
$OldAsSetting = Get-AzAnalysisServicesServer -ResourceGroupName $ResourceGroupName -Name $AnalysisServerName
$OldStatus = $OldAsSetting.State 
 
# $OldStatus
# Paused, Succeeded

if ($OldStatus -eq "Paused")
{
    Write-Output "Resuming $($AnalysisServerName). Current status: $($OldStatus)" 
    Resume-AzAnalysisServicesServer -ResourceGroupName $ResourceGroupName -Name $AnalysisServerName
    
    # Show done when finished (for testing/logging purpose only)
    Write-Output "Done"
}

else
{
    Write-Output "Already Running"
}