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

$currentConfig =  (Get-AzAnalysisServicesServer -ResourceGroupName $ResourceGroupName -Name $AnalysisServerName)[0].FirewallConfig
$currentFirewallRules = $currentConfig.FirewallRules

$Stoploop = $false
[int]$Retrycount = "0"
do{
    try{
        $response = Invoke-WebRequest -uri ifconfig.co/ip  -UseBasicParsing  
        $ip = $response.Content.Trim()
        Write-Output $ip
        $NewAzureAutomationFirewallRule = New-AzAnalysisServicesFirewallRule -FirewallRuleName 'AzureAutomation' -RangeStart $ip -RangeEnd $ip
        if($NewAzureAutomationFirewallRule.RangeStart -in $currentFirewallRules.RangeStart){
            Write-Output "AzureAutomation Firewall IP is up to date!"
        }
        else{
            $newFirewallRules = $currentFirewallRules
            $newFirewallRules.RemoveAll({$args[0].FirewallRuleName -eq 'AzureAutomation'})
            $newFireWallRules.add($NewAzureAutomationFirewallRule)
            $newFireWallConfig = New-AzAnalysisServicesFirewallConfig -FirewallRule $newFirewallRules -EnablePowerBIService
            Set-AzAnalysisServicesServer -Name $AnalysisServerName -ResourceGroupName $ResourceGroupName -FirewallConfig $newFireWallConfig
            Write-Output "AzureAutomation FireWallRule updated with IP $ip !"
        }
        $Stoploop = $true
    }
    catch{
        if($Retrycount -gt 3){
            Write-Warning "Could not retrieve IP after 4 retries."
            $Stoploop = $true
        }
        else{
            Start-Sleep -Seconds 10
            $Retrycount = $Retrycount + 1
            $ErrorMessage = $_.exception.message
            Write-Error "Error during IPAddressManagementAAS: $ErrorMessage"
        }
    }
}
while ($Stoploop -eq $false)

    
    