#.............................................................................................#
Function RandomizeList {
Param (
[Object[]]$list

)
    $randomize=@{}
    $list|ForEach-Object {
        $a=(Get-Random)
        $randomize[$a]=$_
    }
    $randomList=@()
    $randomize.Keys|ForEach-Object {
        $randomList+=$randomize[$_]
    }
    return @(,($randomList))
}
#.............................................................................................#
Function pingJob {
Param (
    [Int]$throttleLimit
)
    if ($throttleLimit -le 0)
    {
        $throttleLimit=10
    }
    
    foreach($Item in $Input){
        Test-Connection -ComputerName $item -Count 1 -BufferSize 1  -AsJob -ThrottleLimit $throttleLimit -ErrorAction SilentlyContinue |Out-Null
    }
}
#.............................................................................................#
Function Out-SQLDataTable 
{
  $lf=[char]10
  $cr=[char]13
  $filler=" "
  $emptyValue="N/A"
  $dt = new-object Data.datatable  
  $first = $true  
  foreach ($item in $input){  
    $dr = $dt.NewRow()  
    $item.PsObject.get_properties() | foreach {  
      if ($first) {  
        $col =  new-object Data.DataColumn  
        $col.ColumnName = $_.Name.ToString()
        $dt.Columns.Add($col)       }  
      if ($_.value -eq $null) {  
        $dr.Item($_.Name) = $emptyValue 
      }  
      elseif ($_.IsArray) {  
        $dr.Item($_.Name) =([string]::Join($_.value ,";")).Replace($cr,$filler).Replace($lf,$filler)
      }
      elseif ($_.Value.Count -gt 1) {  
        $dr.Item($_.Name) =($_.value|Out-String).Replace($cr,$filler).Replace($lf,$filler)
        
      }  
      else {  
        
        $dr.Item($_.Name) = ($_.value|Out-String).Trim()
      }  
    }  
    $dt.Rows.Add($dr)  
    $first = $false  
  } 
 
  return @(,($dt))
}

#.............................................................................................#
Function CreatevdiJob #Creates a job on Remote Host to gather vdi info
{
    Param 
    (
       [String]$class,
       [String]$remoteHost, #Delivery Controller 
       [Int]$maxRecordCount
  
    )
     #Required
    #Name        : Citrix.Broker.Admin.V2
    #PSVersion   : 2.0
    #Description : This PowerShell snap-in contains cmdlets used to manage the Citrix Broker.
    #Get-PSSnapin -Registered -Name "Citrix.Broker.Admin.V2"|Add-PSSnapin
    
   
    $vdiClass=$class
    $vdiObject=$null
    if($maxRecordCount -eq 0){
        $maxRecordCount=2000
    }


    
   $cmd= "Get-"+$vdiClass+" -AdminAddress " +$remoteHost + " -maxRecordCount " +$maxRecordCount
   $vdiObjectJob=Start-Job -InitializationScript {Get-PSSnapin -Registered -Name "Citrix.Broker.Admin.V2"|Add-PSSnapin} -ScriptBlock {Invoke-Expression -Command $args[0]} -ArgumentList $cmd -Name $class #Class stored in Job Name
   return @(,($vdiObjectJob))
}

#.............................................................................................#
Function FormatvdiObjectItem #Detects vdi class and returns SQL friendly formatted output
{
    Param 
    (
       [PSObject]$vdiObject,
       [String]$randomStamp,
       [String]$timeStamp,
       [String]$class
    )
    


    $classlist=("BrokerSession","BrokerDesktop")
    
    $classFieldlist=@{}
    $classFieldlist["BrokerSession"]=("CatalogName","ClientAddress","ClientName","ClientPlatform","ClientVersion","ConnectedViaHostName","ConnectedViaIP","ControllerDNSName","DesktopGroupName","DeviceId","DNSName","EstablishmentDuration","EstablishmentTime","HardwareId","HostedMachineName","HostingServerName","HypervisorConnectionName","IPAddress","IsPhysical","LaunchedViaHostName","OSType","ProvisioningType","ReceiverIPAddress","ReceiverName","UserName","UserUPN")
    $classFieldlist["BrokerDesktop"]=("CatalogName","ControllerDNSName","DeliveryType","DesktopGroupName","HostedMachineName","HostingServerName","HypervisorConnectionName","IPAddress","LastConnectionTime","LastConnectionUse","MachineName", "OSType","OSVersion","ProvisiningType","RegistrationState")
 
    $lf=[char]10
    $cr=[char]13
    $filler=" "
    $emptyValue="N/A"
    $info=$null
    
       
    
    $info=New-Object -TypeName PSobject
        
    $classFieldlist[$class]|ForEach-Object {
        $fieldName=$_
        $fieldValue=$emptyValue

        if ($vdiObject.$fieldName.length -ge 1) {
            $fieldValue=($vdiObject.$fieldName|Out-String).Replace($cr,$filler).Replace($lf,$filler).Trim()
        }
            

        $info|Add-Member -Name $fieldName -Value $fieldValue  -MemberType NoteProperty
    }

    
    #Add stamps
    $info|Add-Member -Name RunRandomStamp -Value $randomStamp  -MemberType NoteProperty
    $info|Add-Member -Name RunTimeStamp -Value $timeStamp  -MemberType NoteProperty
    return @(,($info))
}
#.............................................................................................#


Function FormatvdiObjectJob #Collects a specific job, format data and returns formatted data according to the class.
{
    Param (
    [Object]$vdiObjectJob,
    [String]$randomStamp,
    [String]$timeStamp
    )
    $hostInfo=$null
    $arrayHostInfo=@()
    $arrayHostInfo.Clear()

    #$vdiObjectJob|Get-Job|Wait-Job |Out-Null
    $vdiObject=$vdiObjectJob|Receive-Job -ErrorAction SilentlyContinue
    $class=$vdiObjectJob.Name #Class stored in job name
    
    
    if ($vdiObject -ne $null){
        if ($vdiObject.Length -ne $null) #vdiObject is an array
        {
  
            $vdiObject|ForEach-Object {
                    $vdiObjectItem=$_
                    
                    $arrayHostInfo+=FormatvdiObjectItem -vdiObject $vdiObjectItem -randomStamp $randomStamp -timeStamp $timeStamp -class $class
            }
            return @(,($arrayHostInfo))
  
        }
        else { #Single object
            $hostInfo=FormatvdiObjectItem -vdiObject $vdiObject -randomStamp $randomStamp -timeStamp $timeStamp -class $class
            return @(,($hostInfo))
        }
    }
    

    
}
#.............................................................................................#
Function CreatevdiObjectJobFromComputerList  #Create jobs and store them in $jobMatrix, index is computer Name. For each computer in the list, one job per vdi class queried
{
    Param (
    [String[]]$computerList,
    [String[]]$classList,
    [String]$randomStamp,
    [String]$timeStamp,
    [Int] $jobThrottleLimit,
    [HashTable] $jobMatrix
    )
    
    
    $jobCounter=0

    $computerList|ForEach-Object {
        $endpoint=$_
        $jobMatrix[$endpoint]=@()
        $classlist|ForEach-Object {
            $class=$_
            $jobMatrix[$endpoint]+=CreatevdiJob -class $class -remoteHost $endpoint -jobThrottleLimit  $jobThrottleLimit
            $jobcounter++;
            
        }
    }
}


#.............................................................................................#
Function CollectDataFromJobListByClass  #Collect data from capturedJobs and store them in $dataMatrix, data index is the class Name. 
{
    Param (
    [Object []]$jobList,
    [String]$randomStamp,
    [String]$timeStamp,
    [HashTable] $dataMatrix
    )
    
    $jobList|ForEach-Object {
        $vdiObjectJob=$_
        $result=FormatvdiObjectJob -vdiObjectJob $vdiObjectJob -randomStamp $randomStamp -timeStamp $timeStamp
        $result|ForEach-Object {
            $vdiObjectItem=$_
            $class=$vdiObjectJob.Name
            $dataMatrix[$class]+=$vdiObjectItem
        }
     }
 
}


#.............................................................................................#
Function Create-SQLClassTable #Creates a SQL table using class name and CSV data columns names 
{
    Param 
    (
       [Object[]]$dataCSV,
       [String]$csvDelimiter,
       [String[]]$class,
       [String]$SQLServer,
       [String]$SQLDatabase,
       [String]$colMaxLength

    )
    $dataLinesCount=$dataCSV.Count
    if ($dataLinesCount -gt 0)
    {
        #Find Columns
        $columNames=$dataCSV[0]
        $columNamesList=$columNames.Split($csvDelimiter)
        $rows=$dataCSV[1..($dataLinesCount-1)]
        $columNames|Out-File ($env:Tmp+"\log.txt") -Append
        #Generate SQL Create script from class Name and columns names
               
        $stringSQLType="NVARCHAR("+$colMaxLength+")"

        $sqlScript=""
        

        $sqlScript+="CREATE TABLE "+$class #Table name is the class name
        $sqlScript+="("                    #Open caption
        $columNamesList|ForEach-Object {
            $sqlScript+="["+$_+"]" + " "+$stringSQLType +","
        }
        $sqlScript = $sqlScript.TrimEnd(",")          #Remove last ,
        $sqlScript+=")"                  #Close caption
        
        #Invoke-sqlcmd with script  using connection string parameters
        Invoke-Sqlcmd -ServerInstance $SQLServer -Database $SQLDatabase $sqlScript
    }
}

#MAIN BODY#
#............................................................................................................................#
$startTime=Get-Date
$captureJob=@{}
$randomStamp=(Get-Random).Tostring()
$timeStamp=(Get-Date).Tostring()
$jobThrottleLimit=30 #Based on heuristics 
$vdiCSVFolder="V:\tmp\vdiCSVload"
$delieveryControllers=("ithaca-xd1")


$createSQLTables=$false  #Switch to create SQL tables
$captureData=$true       #Switch to capture data from computer list, needed to create tables
$loadData=$true          #Switch to load existing CSV data from folder
$deleteAfterLoad=$true
$archiveFolderName=($vdiCSVFolder+"\Archive")
$classList=("BrokerSession","BrokerDesktop")


Get-Job|Remove-Job -Force #Clean jobs



if ($captureData) {

    "Gathering data..." #Console message

    #Create vdi data capture jobs................................................................................................
    $jobMatrix=@{}
    CreatevdiObjectJobFromComputerList -computerList $delieveryControllers -classlist $classList -randomStamp $randomStamp -timeStamp $timeStamp -jobThrottleLimit $jobThrottleLimit -jobMatrix  $jobMatrix  


    #Collect vdi data jobs results...............................................................................................
    $jobList=Get-Job|sort State

    "Saving Data..." #Console message
    $jobList.Count


    #Initialize data matrix......................................................................................................
    $dataMatrix=@{} 

    #By computer(location)
    #$testlist|ForEach-Object {
    #    $dataMatrix[$_]=@()
    #}
    #CollectDataFromJobListByLocation  -jobList $jobList  -randomStamp $randomStamp -timeStamp $timeStamp -dataMAtrix $dataMatrix 

    #By class
    $classlist|ForEach-Object {
        $dataMatrix[$_]=@()
    }

    $jobsTimeOut=900 #In seconds, 300=5 minutes, 600=10 minutes, etc.
    #Wait for jobs to finish
    Get-Job|Wait-Job -Timeout  $jobsTimeOut |Out-Null
    $jobList=Get-Job -State Completed

    CollectDataFromJobListByClass  -jobList $jobList  -randomStamp $randomStamp -timeStamp $timeStamp -dataMAtrix $dataMatrix 

    #Create SQL comaptible tables from data.................................................................................................
    $tablesMatrix=@{} 
    $classlist|ForEach-Object {
        $tablesMatrix[$_]=@()
    }

    $dataMatrix.Keys|ForEach-Object {
        $class=$_
        $tablesMatrix[$class]=($dataMatrix[$class]|Out-SQLDataTable)
      }


    #Convert SQL tables to CSV for exporting or BulkCopy to SQL server...........................................................
    $dataCSV=@{} 
    $csvDelimiter=","
    $tablesMatrix.Keys|ForEach-Object {
        $class=$_
        $dataCSV[$class]=$tablesMatrix[$class]|ConvertTo-Csv -Delimiter $csvDelimiter -NoTypeInformation

      }

    #Export to CSV files..........................................................................................................


    $dataCSV.Keys|ForEach-Object {
        $class=$_
        $outputFile=$vdiCSVFolder+"\"+$class+"-"+$randomStamp+".CSV"
        $dataCSV[$class]|Set-Content $outputFile -ErrorAction SilentlyContinue

      }
}

#Create SQL Tables............................................................................................................


$colMaxLength=600
$SQLServer="sql-8402\SQLExpressPS"
$SQLDAtabase="sessionsVDI"

if ($createSQLTables -eq $true) {
      $dataCSV.Keys|ForEach-Object {
      $class=$_
      Create-SQLClassTable -class $class -dataCSV $dataCSV[$class] -csvDelimiter $csvDelimiter -SQLServer $SQLServer -SQLDatabase $sqlDatabase -colMaxLength $colMaxLength
      }

}

#Load data from existing CSV into SQL database............................................................................................

if ($loadData)
    {
    $archiveFolder=New-Item $archiveFolderName -ItemType Directory -Force -ErrorAction SilentlyContinue
    $csvFiles=get-childitem  $vdiCSVFolder |where {$_.Name -like "*-*.CSV"}

    $connectionString = "Data Source="+$SQLServer+"; Integrated Security=True;Initial Catalog="+$SQLDataBase+";"
    $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $connectionString

    $preSQLdata=@{}
    $SQLdata=@{}

    $csvFiles|ForEach-Object{
        $class=$_.Name.Split("-")[0]
        $csvFile=$_
        $csvFile
        
        $preSQLdata[$class]=$_|Get-Content|ConvertFrom-Csv -Delimiter $csvDelimiter
        $SQLdata[$class]=($preSQLdata[$class]|Out-SQLDataTable)

        #Save to SQL
        $bulkCopy.DestinationTableName = $class
        $errCode=$bulkCopy.WriteToServer($SQLdata[$class]) 
        #$errCode
        if ($deleteAfterLoad) {
            if ($archiveFolder.Exists) {
                $csvFile|Move-Item -Destination $archiveFolderName -Force -ErrorAction SilentlyContinue
                #$csvFiles|Remove-Item -Force -ErrorAction SilentlyContinue
            }
        }

    }
    


}

$finishTime=Get-Date
$duration=($finishTime-$startTime)
"Duration(minutes):"+ [Math]::Floor($duration.TotalMinutes)

