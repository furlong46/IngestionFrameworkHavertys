param
(
    [Parameter (Mandatory = $false)]
    [object] $WebHookData,

    [Parameter (Mandatory = $false)]
    [String] $AnalysisServer,
    
    [Parameter (Mandatory = $false)]
    [String] $DatabaseName

)

$Connection = Get-AutomationConnection -Name "AzureRunAsConnection"
$TenantId = $Connection.TenantId
$SubscriptionId = $Connection.SubscriptionId
$ResourceGroupName = Get-AutomationVariable -Name "ResourceGroupName"

# Get Azure Credential
$AzureCred = Get-AutomationPSCredential -Name 'DataAnalyticsQAServicePrincipal'

# Add AAS Service Account
Write-Output "Login to Azure using automation account 'ASRefreshCred'."
Add-AzureAnalysisServicesAccount -RolloutEnvironment 'eastus.asazure.windows.net' -ServicePrincipal -Credential $AzureCred -TenantId $TenantId


#Start AAS
Connect-AzureRmAccount -ServicePrincipal -Tenant $Connection.TenantID -ApplicationID $Connection.ApplicationID -CertificateThumbprint $Connection.CertificateThumbprint
$AzureContext = Select-AzureRmSubscription -SubscriptionId $connection.SubscriptionID
$AutomationAcct = Get-AutomationVariable -Name 'AutomationServiceName'

Start-AzureRmAutomationRunbook -AutomationAccountName $AutomationAcct -Name "ResumeAAS" -ResourceGroupName $ResourceGroupName -MaxWaitSeconds 1000 -Wait

#Process AAS Incrementally Based upon Manual or an Automated Job (if the Webhook is used then it is an automated job)
if ($WebhookData)
{ 
    # Retrieve AAS details from Webhook request body
    $atmParameters = (ConvertFrom-Json -InputObject $WebhookData.RequestBody)
    
    if ($atmParameters.AnalysisServicesDatabaseName) {
        $_databaseName = $atmParameters.AnalysisServicesDatabaseName
        Write-Output "AnalysisServicesDatabaseName: $($atmParameters.AnalysisServicesDatabaseName)"
    }

    if ($atmParameters.AnalysisServicesServer) {
        $_analysisServer = $atmParameters.AnalysisServicesServer
        Write-Output "AnalysisServicesServer: $($atmParameters.AnalysisServicesServer)"
    }
    
    if ($atmParameters.callBackUri) {
        $callBackUri = $atmParameters.callBackUri
        Write-Output "callBackUri: $($atmParameters.callBackUri)"
    }

    #Full Process the Database from Webhook
    Write-Output "Process AAS DB $($DatabaseName) from Webhook"
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Color Component"										
    Write-Output "Processing complete for Common Color Component"							
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Color SKU Sold"                                      
    Write-Output "Processing complete for Common Color SKU Sold"                            
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Finish Component"                                    
    Write-Output "Processing complete for Common Finish Component"                          
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Finish SKU Sold"                                     
    Write-Output "Processing complete for Common Finish SKU Sold"                           
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Credit Promotion"                                           
    Write-Output "Processing complete for Credit Promotion"                                 
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Date"                                                       
    Write-Output "Processing complete for Date"                                             
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Delivery DC"                                                
    Write-Output "Processing complete for Delivery DC"                                      
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Designer"                                                   
    Write-Output "Processing complete for Designer"                                         
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Designer Team"                                              
    Write-Output "Processing complete for Designer Team"                                    
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Factory"                                                    
    Write-Output "Processing complete for Factory"                                          
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Location"                                                   
    Write-Output "Processing complete for Location"                                         
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Materials Component"                                        
    Write-Output "Processing complete for Materials Component"                              
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Materials SKU Sold"                                         
    Write-Output "Processing complete for Materials SKU Sold"                               
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Merchandising Promotion"                                    
    Write-Output "Processing complete for Merchandising Promotion"                          
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Override Reason"                                            
    Write-Output "Processing complete for Override Reason"                                  
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Sales Associate"                                            
    Write-Output "Processing complete for Sales Associate"                                  
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Sales Team"                                                
    Write-Output "Processing complete for Sales Team"                                       
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Special Order Attribute Key Component"                      
    Write-Output "Processing complete for Special Order Attribute Key Component"            
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Special Order Key Value Component"                          
    Write-Output "Processing complete for Special Order Key Value Component"                
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Time"                                                       
    Write-Output "Processing complete for Time"                                             
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Time Intelligence"                                          
    Write-Output "Processing complete for Time Intelligence"                                
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Vendor"                                                     
    Write-Output "Processing complete for Vendor"                                           
    Invoke-ProcessPartition  -Database $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Customer" -PartitionName "Customer Current"                 
    Write-Output "Processing complete for Customer Partition Customer Current"              
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "SKU Component"                                              
    Write-Output "Processing complete for SKU Component"                                    
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "SKU Sold"                                                   
    Write-Output "Processing complete for SKU Sold"                                         
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Special Order Attributes"                                   
    Write-Output "Processing complete for Special Order Attributes"                         
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Attached SKU Component"                                       
    Write-Output "Processing complete for Attached SKU Component"                             
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Sales Team Bridge"                                          
    Write-Output "Processing complete for Sales Team Bridge"                                
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Designer Team Bridge"                                       
    Write-Output "Processing complete for Designer Team Bridge"                             
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Finish Bridge Component"                             
    Write-Output "Processing complete for Common Finish Bridge Component"                   
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Finish Bridge SKU Sold"                              
    Write-Output "Processing complete for Common Finish Bridge SKU Sold"                    
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Color Bridge Component"                              
    Write-Output "Processing complete for Common Color Bridge Component"                    
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Common Color Bridge SKU Sold"                               
    Write-Output "Processing complete for Common Color Bridge SKU Sold"                     
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Material Bridge SKU Sold"                                   
    Write-Output "Processing complete for Material Bridge SKU Sold"                         
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Materials Bridge Component"                                 
    Write-Output "Processing complete for Materials Bridge Component"                       
    Invoke-ProcessTable -DatabaseName $_databaseName -RefreshType Full -Server $_analysisServer -TableName "SKU Cost"                                                   
    Write-Output "Processing complete for SKU Cost"                                         
    Invoke-ProcessPartition  -Database $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Delivered Sales" -PartitionName "Delivered Sales Current"   
    Write-Output "Processing complete for Delivered Sales Partition Delivered Sales Current"
    Invoke-ProcessPartition  -Database $_databaseName -RefreshType Full -Server $_analysisServer -TableName "Written Sales" -PartitionName "Written Sales Current"       
    Write-Output "Processing complete for Written Sales Partition Written Sales Current"    

    #Call back to ADF
    if ($callBackUri) {
        Invoke-WebRequest -Uri $callBackUri -Method POST -UseBasicParsing
    }
}
else 
{
    #Full Process the Database from Parameters
    Write-Output "Process AAS DB $($DatabaseName) from Parameters"
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Color Component"									
    Write-Output "Processing complete for Common Color Component"									
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Color SKU Sold"                                    
    Write-Output "Processing complete for Common Color SKU Sold"                                   
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Finish Component"                                  
    Write-Output "Processing complete for Common Finish Component"                                 
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Finish SKU Sold"                                   
    Write-Output "Processing complete for Common Finish SKU Sold"                                  
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Credit Promotion"                                         
    Write-Output "Processing complete for Credit Promotion"                                        
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Date"                                                     
    Write-Output "Processing complete for Date"                                                    
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Delivery DC"                                              
    Write-Output "Processing complete for Delivery DC"                                             
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Designer"                                                 
    Write-Output "Processing complete for Designer"                                                
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Designer Team"                                            
    Write-Output "Processing complete for Designer Team"                                           
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Factory"                                                  
    Write-Output "Processing complete for Factory"                                                 
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Location"                                                 
    Write-Output "Processing complete for Location"                                                
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Materials Component"                                      
    Write-Output "Processing complete for Materials Component"                                     
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Materials SKU Sold"                                       
    Write-Output "Processing complete for Materials SKU Sold"                                      
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Merchandising Promotion"                                  
    Write-Output "Processing complete for Merchandising Promotion"                                 
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Override Reason"                                          
    Write-Output "Processing complete for Override Reason"                                         
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Sales Associate"                                          
    Write-Output "Processing complete for Sales Associate"                                         
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Sales Team"                                               
    Write-Output "Processing complete for Sales Team"                                              
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Special Order Attribute Key Component"                    
    Write-Output "Processing complete for Special Order Attribute Key Component"                   
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Special Order Key Value Component"                        
    Write-Output "Processing complete for Special Order Key Value Component"                       
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Time"                                                     
    Write-Output "Processing complete for Time"                                                    
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Time Intelligence"                                        
    Write-Output "Processing complete for Time Intelligence"                                       
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Vendor"                                                   
    Write-Output "Processing complete for Vendor"                                                  
    Invoke-ProcessPartition  -Database $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Customer" -PartitionName "Customer Current"               
    Write-Output "Processing complete for Customer Partition Customer Current"              
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "SKU Component"                                            
    Write-Output "Processing complete for SKU Component"                                           
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "SKU Sold"                                                 
    Write-Output "Processing complete for SKU Sold"                                                
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Special Order Attributes"                                 
    Write-Output "Processing complete for Special Order Attributes"                                
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Attached SKU Component"                                     
    Write-Output "Processing complete for Attached SKU Component"                                    
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Sales Team Bridge"                                        
    Write-Output "Processing complete for Sales Team Bridge"                                       
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Designer Team Bridge"                                     
    Write-Output "Processing complete for Designer Team Bridge"                                    
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Finish Bridge Component"                           
    Write-Output "Processing complete for Common Finish Bridge Component"                          
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Finish Bridge SKU Sold"                            
    Write-Output "Processing complete for Common Finish Bridge SKU Sold"                           
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Color Bridge Component"                            
    Write-Output "Processing complete for Common Color Bridge Component"                           
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Common Color Bridge SKU Sold"                             
    Write-Output "Processing complete for Common Color Bridge SKU Sold"                            
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Material Bridge SKU Sold"                                 
    Write-Output "Processing complete for Material Bridge SKU Sold"                                
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Materials Bridge Component"                               
    Write-Output "Processing complete for Materials Bridge Component"                              
    Invoke-ProcessTable -DatabaseName $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "SKU Cost"                                                 
    Write-Output "Processing complete for SKU Cost"                                                
    Invoke-ProcessPartition  -Database $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Delivered Sales" -PartitionName "Delivered Sales Current"	
    Write-Output "Processing complete for Delivered Sales Partition Delivered Sales Current"
    Invoke-ProcessPartition  -Database $DatabaseName -RefreshType Full -Server $AnalysisServer -TableName "Written Sales" -PartitionName "Written Sales Current"     
    Write-Output "Processing complete for Written Sales Partition Written Sales Current"    

}