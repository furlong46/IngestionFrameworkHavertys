param
(
    [Parameter (Mandatory = $false)]
    [object] $WebHookData,

    [Parameter (Mandatory = $false)]
    [String] $AnalysisServer,
    
    [Parameter (Mandatory = $false)]
    [String] $DatabaseName

)

$headers = @{
    "Content-Type"="application/json"
}

# Call Back URI Status Body
# Failure
$BodyJSONFailure = @"
{
    "Output": {
        "ProcessingStatus": "Failure"
    },
    "Error": {
        "ErrorCode": "Failure",
        "Message": "AAS Processing Failed"
    },
    "StatusCode": "403"
}
"@ | ConvertFrom-Json

# Success
$BodyJSONSuccess = @"
{
    "Output": {
        "ProcessingStatus": "Success"
    },
    "Error": {},
    "StatusCode": "300"
}
"@ | ConvertFrom-Json


# Default response to fail
$JSONResponse = $BodyJSONFailure 

Write-Output "Full Process AAS start" 

# Parse WebhookData
if ($WebhookData)
{ 
    Write-Output "Parsing WebhookData"

    # Retrieve AAS details from Webhook request body
    $atmParameters = (ConvertFrom-Json -InputObject $WebhookData.RequestBody)
    
    if ($atmParameters.AnalysisServicesDatabaseName) 
    {
        $_databaseName = $atmParameters.AnalysisServicesDatabaseName
        Write-Output "AnalysisServicesDatabaseName: $($atmParameters.AnalysisServicesDatabaseName)"
    }

    if ($atmParameters.AnalysisServicesServer) 
    {
        $_analysisServer = $atmParameters.AnalysisServicesServer
        Write-Output "AnalysisServicesServer: $($atmParameters.AnalysisServicesServer)"
    }
    
    if ($atmParameters.callBackUri) 
    {
        $callBackUri = $atmParameters.callBackUri
        Write-Output "callBackUri: $($atmParameters.callBackUri)"
    }
}


# Get Azure Configuration
$Connection = Get-AutomationConnection -Name "AzureRunAsConnection"
$TenantId = $Connection.TenantId
$SubscriptionId = $Connection.SubscriptionId
$ResourceGroupName = Get-AutomationVariable -Name "ResourceGroupName"

# Get Azure Credential
$AzureCred = Get-AutomationPSCredential -Name 'DataAnalyticsQAServicePrincipal'

# Add AAS Service Account
Write-Output "Login to Azure using automation account 'ASRefreshCred'."
Add-AzureAnalysisServicesAccount -RolloutEnvironment 'eastus.asazure.windows.net' -ServicePrincipal -Credential $AzureCred -TenantId $TenantId

try 
{
    # Start AAS
    Write-Output "Connecting to AAS account .."
    Connect-AzureRmAccount -ServicePrincipal -Tenant $Connection.TenantID -ApplicationID $Connection.ApplicationID -CertificateThumbprint $Connection.CertificateThumbprint
    $AzureContext = Select-AzureRmSubscription -SubscriptionId $connection.SubscriptionID
    $AutomationAcct = Get-AutomationVariable -Name 'AutomationServiceName'

    Write-Output "Start ResumeAAS runbook .."
    Start-AzureRmAutomationRunbook -AutomationAccountName $AutomationAcct -Name "ResumeAAS" -ResourceGroupName $ResourceGroupName -MaxWaitSeconds 1000 -Wait -EA Stop
    Write-Output "End ResumeAAS runbook .."

    Write-Output "Start IPAddressManagementAAS runbook .."
    Start-AzureRmAutomationRunbook -AutomationAccountName $AutomationAcct -Name "IPAddressManagementAAS" -ResourceGroupName $ResourceGroupName -MaxWaitSeconds 1000 -Wait -EA Stop
    Write-Output "End IPAddressManagementAAS runbook .."


    # Process AAS Incrementally Based upon Manual or an Automated Job (if the Webhook is used then it is an automated job)
    Write-Output "Begin processing AAS .."
    if ($WebhookData)
    {
        #Full Process the Database
        Write-Output "Processing AAS DB $($_databaseName) start"
        Invoke-ProcessASDatabase -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -EA Stop
        Write-Output "Processing AAS DB $($_databaseName) end"

        $JSONResponse  = $BodyJSONSuccess

        #Call back to ADF
        if ($callBackUri) 
        {
            Write-Output "Invoking webrequest to $callBackUri with response code $JSONResponse.StatusCode"
            Invoke-WebRequest -Uri $callBackUri -UseBasicParsing -Method POST -Body ($JSONResponse|ConvertTo-Json) -Header $headers
        }
    }
    else 
    {
        if ($DatabaseName -and $AnalysisServer)
        {
            Write-Output "Processing AAS DB $($DatabaseName) start"
            Invoke-ProcessASDatabase -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -EA Stop
            Write-Output "Processing AAS DB $($DatabaseName) end"

            $JSONResponse  = $BodyJSONSuccess
        }
        else 
        {
            $buildMissingInputString = New-Object -TypeName System.Text.StringBuilder
            $buildMissingInputString.Append("Cannot process AAS DB. Please enter the following missing input(s): ")
            if (!$DatabaseName) 
            { 
                $buildMissingInputString.Append("DatabaseName")
            }
            if (!$AnalysisServer) 
            { 
                if (!$DatabaseName) 
                { 
                    $buildMissingInputString.Append(", ")
                }
                $buildMissingInputString.Append("AnalysisServer")
            } 
            
            throw $buildMissingInputString.ToString()
        }
    }
}
catch 
{
    $ErrorMessage = $_.Exception.Message
    $FailedItem = $_.Exception.ItemName
    $JSONResponse = $BodyJSONFailure 
    
    Write-Error "Caught Exception - Error Message: $ErrorMessage, Failed Item: $FailedItem"
    if ($callBackUri) 
    {
        Write-Output "Invoking webrequest to $callBackUri with response code $JSONResponse.StatusCode"        
        Invoke-WebRequest -Uri $callBackUri -UseBasicParsing -Method POST -Body ($JSONResponse|ConvertTo-Json) -Header $headers
    }
    else 
    {
        Write-Error "Call back fail - Empty callBackUri"
    }
    throw $ErrorMessage
}
finally
{
    $StatusCode = $JSONResponse.StatusCode
    Write-Output "Full Process AAS End" 
    if ($StatusCode -eq  300) 
    { 
        Write-Warning "AAS processing was succesful with a status code $StatusCode!" 
    }
    else 
    { 
        Write-Warning "AAS processing failed with a status code $StatusCode!"  
    }
}

