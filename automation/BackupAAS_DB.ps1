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
Import-Module -Name SqlServer

#Get password
$ResourceGroupName = Get-AutomationVariable -Name "ResourceGroupName"
$AnalysisServerName = Get-AutomationVariable -Name 'AnalysisServerName'
$AnalysisServerNameFull = Get-AutomationVariable -Name 'AnalysisServerNameFull'
$SemanticLayerDBName = Get-AutomationVariable -Name 'HavertysSemanticLayerDBName'

# Backup DB
$Date = Get-Date
$DateStr = $Date.ToString("yyyyMMdd")
$FileName = $SemanticLayerDBName + $DateStr + ".abf"
Write-Output "Backing up to $($FileName)"
$Encrypted = ConvertTo-SecureString "Havertys123" -AsPlainText -Force 
backup-asdatabase -backupfile $FileName -name $SemanticLayerDBName -filepassword $Encrypted -server $AnalysisServerNameFull -Credential $AzureCred -ServicePrincipal -AllowOverwrite
 
# Show done when finished (for testing/logging purpose only)
Write-Output "Done"
