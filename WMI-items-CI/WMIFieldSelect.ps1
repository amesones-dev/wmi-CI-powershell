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
Function CreateWMIJob #Creates a job on Remote Host to gather WMI info
{
    Param 
    (
       [String]$class,
       [String]$remoteHost,
       [Int]$jobThrottleLimit
  
    )
   
    $wmiClass="Win32_"+$class
    $wmiObject=$null

   # Wait random time based on interval and random tolerance
   # $jobInterval=0
   # $jobIntervalRnd=0.25
   # $whenLaunched=$jobInterval+(Get-Random -Maximum $jobIntervalRnd)
   # Wait-Event -Timeout $whenLaunched 
    

    $wmiObjectJob=Invoke-Command -ScriptBlock {gwmi -class $args[0] -ComputerName $args[1] -AsJob -ThrottleLimit $args[2]} -ArgumentList $wmiClass,$remoteHost, $jobThrottleLimit
    return @(,($wmiObjectJob))
}

#.............................................................................................#
Function FormatWMIObjectItem #Detects WMI class and returns SQL friendly formatted output
{
    Param 
    (
       [PSObject]$wmiObject,
       [String]$randomStamp,
       [String]$timeStamp
    )
    
    $classlist=("ComputerSystem","ComputerSystemProduct","OperatingSystem","NetworkADapterConfiguration","PnPEntity","Product")
    
    $classFieldlist=@{}
    $classFieldlist["ComputerSystem"]=("__SERVER","__CLASS","Name","Domain","Manufacturer","Model","PrimaryOwnerName", "SystemFamily","TotalPhysicalMemory","UserName")
    $classFieldlist["ComputerSystemProduct"]=("__SERVER","__CLASS","Vendor","Name","Version","IdentifyingNumber")
    $classFieldlist["OperatingSystem"]=("__SERVER","__CLASS","Manufacturer","Caption","OSArchitecture","Version","LastBootUpTime","MUILanguages","Locale")
    $classFieldlist["NetworkAdapterConfiguration"]=("__SERVER","__CLASS","Description","DHCPEnabled","DHCPServer","DNSDomain","DNSDomainSuffixSearchOrder","DNSServerSearchOrder","DomainDNSRegistrationEnabled","FullDNSRegistrationEnabled","IPAddress","DefaultIPGateway","IPSubnet","MACAddress","ServiceName")
    $classFieldlist["PNPEntity"]=("__SERVER","__CLASS","Caption","Manufacturer", "PNPClass","Service")
    $classFieldlist["Product"]=("__SERVER","__CLASS","Caption","Vendor", "Version","InstallDate")
 
    $lf=[char]10
    $cr=[char]13
    $filler=" "
    $emptyValue="N/A"
    $info=$null
    
    if ($wmiObject.__CLASS.StartsWith("Win32_"))
    {
        $class=$wmiObject.__CLASS.Substring("Win32_".Length)
        $info=New-Object -TypeName PSobject
        
        $classFieldlist[$class]|ForEach-Object {
            $fieldName=$_
            $fieldValue=$emptyValue

            if ($wmiObject.$fieldName.length -ge 1) {
                $fieldValue=($wmiObject.$fieldName|Out-String).Replace($cr,$filler).Replace($lf,$filler).Trim()
            }
            

            $info|Add-Member -Name $fieldName -Value $fieldValue  -MemberType NoteProperty
        }

    }
    #Add stamps
    $info|Add-Member -Name RunRandomStamp -Value $randomStamp  -MemberType NoteProperty
    $info|Add-Member -Name RunTimeStamp -Value $timeStamp  -MemberType NoteProperty
    return @(,($info))
}
#.............................................................................................#


Function FormatWMIObjectJob #Collects a specific job, format data and returns formatted data according to the class.
{
    Param (
    [PSObject]$wmiObjectJob,
    [String]$randomStamp,
    [String]$timeStamp
    )
    $hostInfo=$null
    $arrayHostInfo=@()
    $arrayHostInfo.Clear()

    #$wmiObjectJob|Get-Job|Wait-Job |Out-Null
    $wmiObject=$wmiObjectJob|Receive-Job -ErrorAction SilentlyContinue
    
    if ($wmiObject -ne $null){
        if ($wmiObject.Length -ne $null) #wmiObject is an array
        {
  
            $wmiObject|ForEach-Object {
                    $wmiObjectItem=$_
                    $arrayHostInfo+=FormatWMIObjectItem -wmiObject $wmiObjectItem -randomStamp $randomStamp -timeStamp $timeStamp
            }
            return @(,($arrayHostInfo))
  
        }
        else { #Single object
            $hostInfo=FormatWMIObjectItem -wmiObject $wmiObject -randomStamp $randomStamp -timeStamp $timeStamp
            return @(,($hostInfo))
        }
    }
    

    
}
#.............................................................................................#
Function CreateWMIObjectJobFromComputerList  #Create jobs and store them in $jobMatrix, index is computer Name. For each computer in the list, one job per WMI class queried
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
            $jobMatrix[$endpoint]+=CreateWMIJob -class $class -remoteHost $endpoint -jobThrottleLimit  $jobThrottleLimit
            $jobcounter++;
            #Console message to provide user feedback 
            if($jobCounter%$classlist.Count -eq 0){
                $line="#"+($jobCounter/$classlist.Count).ToString()+" ...... "+ "@"+((get-job -State Running).Count).ToString()
                $line|out-file ($env:TMP+"\line.txt") -Force -Append
                }
        }
    }
}


#.............................................................................................#
Function CollectDataFromJobListByLocation  #Collect data from capturedJobs and store them in $dataMatrix, data index is the computer Name. 
{
    Param (
    [Object []]$jobList,
    [String]$randomStamp,
    [String]$timeStamp,
    [HashTable] $dataMatrix
    )
    
    $jobList|ForEach-Object {
        $wmiObjectJob=$_
        $endpoint=$wmiObjectJob.Location
        $dataMatrix[$endpoint]+=FormatWMIObjectJob -wmiObjectJob $wmiObjectJob -randomStamp $randomStamp -timeStamp $timeStamp
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
        $wmiObjectJob=$_
        $result=FormatWMIObjectJob -wmiObjectJob $wmiObjectJob -randomStamp $randomStamp -timeStamp $timeStamp
        $result|ForEach-Object {
            $wmiObjectItem=$_
            $class=$wmiObjectItem.__CLASS.Substring("Win32_".Length).Trim()
            $dataMatrix[$class]+=$wmiObjectItem
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
$wmiCSVFolder="V:\tmp\wmiCSVload"


$createSQLTables=$false  #Switch to create SQL tables
$captureData=$false       #Switch to capture data from computer list, needed to create tables
$loadData=$true          #Switch to load existing CSV data from folder
$deleteAfterLoad=$true
$archiveFolderName=($wmiCSVFolder+"\Archive")
$classList=("ComputerSystem","ComputerSystemProduct","OperatingSystem","NetworkAdapterConfiguration","PnPEntity","Product")
$csvDelimiter=","



Get-Job|Remove-Job -Force #Clean jobs

#$adpcs=Get-ADComputer -filter {Name -like "PC00*"}
#$adlaptops=Get-ADComputer -filter {Name -like "L00*"}
#$adClients= Get-ADComputer -filter {(Name -like "PC00*") -or (Name -like "L00*")}
#$testList=Get-ADComputer -filter {(Name -like "PC00*") -or (Name -like "L00*")}

#Sample Computers to find online computers..............................................................................................
$sampleList=RandomizeList -list ((Get-ADComputer -filter {(Name -like "PC00*") -or (Name -like "L00*")}).Name)
$testList=@()

$sampleJobs=@()
$sampleJobsTimeOut=30 #30 seconds

$sampleList | pingJob -throttleLimit $jobThrottleLimit
Get-Job|Wait-Job -Timeout $sampleJobsTimeOut |Out-Null
#Select Online computers 
$online =(Get-Job -State Completed|Receive-job)|Where {$_.ResponseTime -ne $null}
$testlist=$online|ForEach-Object {$_.Address}



Get-Job|Remove-job -Force #Clean jobs

if ($captureData) {

    "Gathering data..." #Console message
    $sampleList.Count.ToString() + " endpoints selected"
    $testList.Count.ToString()+ " endpoints online"


    #Create WMI data capture jobs................................................................................................
    $jobMatrix=@{}
    CreateWMIObjectJobFromComputerList -computerList $testlist -classlist $classList -randomStamp $randomStamp -timeStamp $timeStamp -jobThrottleLimit $jobThrottleLimit -jobMatrix  $jobMatrix  


    #Collect WMI data jobs results...............................................................................................
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
    
    $tablesMatrix.Keys|ForEach-Object {
        $class=$_
        $dataCSV[$class]=$tablesMatrix[$class]|ConvertTo-Csv -Delimiter $csvDelimiter -NoTypeInformation

      }

    #Export to CSV files..........................................................................................................


    $dataCSV.Keys|ForEach-Object {
        $class=$_
        $outputFile=$wmiCSVFolder+"\"+$class+"-"+$randomStamp+".CSV"
        $dataCSV[$class]|Set-Content $outputFile -ErrorAction SilentlyContinue

      }
}

#Create SQL Tables............................................................................................................


$colMaxLength=600
$SQLServer="sql-8402\SQLExpressPS"
$SQLDAtabase="itemsCI"

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
    $csvFiles=get-childitem  $wmiCSVFolder |where {$_.Name -like "*-*.CSV"}

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

