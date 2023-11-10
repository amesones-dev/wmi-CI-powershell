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
Function CreateADJob #Creates a job on Remote Host to gather AD info
{
    Param 
    (
       [String]$class,
       [String]$remoteDC,
       [Int]$resultPageSize,
       [Int]$resultSetSize
  
    )
   
    $ADClass=$class #Format ADComputer/ADUser
    $ADObject=$null

    if($resultPageSize -eq 0){
        $resultPageSize=50
    }

    if($resultSetSize -eq 0){
        $resultSetSize=5000
    }

    if ($remoteDC -eq ""){
        $remoteDC=(Get-ADDomainController -Discover).Name
    }

   # Wait random time based on interval and random tolerance
   # $jobInterval=0
   # $jobIntervalRnd=0.25
   # $whenLaunched=$jobInterval+(Get-Random -Maximum $jobIntervalRnd)
   # Wait-Event -Timeout $whenLaunched 

   $cmd= "Get-"+$ADClass+" -filter * -Properties * -ResultPageSize " +$resultPageSize+" -ResultSetSize "+$resultSetSize
   $ADObjectJob=Start-Job -ScriptBlock {Invoke-Expression -Command $args[0]} -ArgumentList $cmd
   
   
   return @(,($ADObjectJob))

    
}

#.............................................................................................#
Function CreateADObjectJobFromDCList  #Create jobs and store them in $jobMatrix, index is computer Name. For each computer in the list, one job per WMI class queried
{
    Param (
    [String[]]$DCList,
    [String[]]$classList,
    [String]$randomStamp,
    [String]$timeStamp,
    [HashTable] $jobMatrix
    )
    
    if ($DCList.Length -le 0){
        $DCList=@()
        $DCList+=(Get-ADDomainController -Discover).Name

    }
    $jobCounter=0

    $DCList|ForEach-Object {
        $remoteDC=$_
        $jobMatrix[$remoteDC]=@()
        $classlist|ForEach-Object {
            $class=$_
            $jobMatrix[$remoteDC]+=CreateADJob -class $class -remoteDC $remoteDC 
        }
    }
}

#.............................................................................................#
Function FormatADObjectItem #Detects WMI class and returns SQL friendly formatted output
{
    Param 
    (
       [PSObject]$ADObject,
       [String]$randomStamp,
       [String]$timeStamp
    )
    
    $classlist=("ADComputer","ADUser")
    
    $classFieldlist=@{}
    $classFieldlist["ADComputer"]=("Name","objectClass","distinguishedName","SID", "whenCreated", "whenChanged", "lastLogonDate", "Enabled")
    $classFieldlist["ADUser"]=("samAccountName","Name","DisplayName","objectClass","distinguishedName","SID", "whenCreated", "whenChanged", "lastLogonDate", "Enabled","homeDirectory","mail","office")
    
    $lf=[char]10
    $cr=[char]13
    $filler=" "
    $emptyValue="N/A"
    $info=$null
    
    $class="AD"+$ADObject.objectclass
    
    $info=New-Object -TypeName PSobject
        
    $classFieldlist[$class]|ForEach-Object {
        $fieldName=$_
        $fieldValue=$emptyValue

        if ($ADObject.$fieldName.length -ge 1) {
            $fieldValue=($ADObject.$fieldName|Out-String).Replace($cr,$filler).Replace($lf,$filler).Trim()
        }
            

        $info|Add-Member -Name $fieldName -Value $fieldValue  -MemberType NoteProperty
    }

    
    #Add stamps
    $info|Add-Member -Name RunRandomStamp -Value $randomStamp  -MemberType NoteProperty
    $info|Add-Member -Name RunTimeStamp -Value $timeStamp  -MemberType NoteProperty
    return @(,($info))
}
#.............................................................................................#
Function FormatADObjectJob #Collects a specific job, format data and returns formatted data according to the class.
{
    Param (
    [PSObject]$ADObjectJob,
    [String]$randomStamp,
    [String]$timeStamp
    )
    $info=$null
    $arrayInfo=@()
    $arrayInfo.Clear()

    #$wmiObjectJob|Get-Job|Wait-Job |Out-Null
    $ADObject=$ADObjectJob|Receive-Job -ErrorAction SilentlyContinue
    
    if ($ADObject -ne $null){
        if ($ADObject.Length -ne $null) #ADObject is an array
        {
  
            $ADObject|ForEach-Object {
                    $ADObjectItem=$_
                    $arrayInfo+=FormatADObjectItem -ADObject $ADObjectItem -randomStamp $randomStamp -timeStamp $timeStamp
            }
            return @(,($arrayInfo))
        }
        else { #Single object
            $info=FormatADObjectItem -ADObject $ADObject -randomStamp $randomStamp -timeStamp $timeStamp
            return @(,($info))
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
        $ADObjectJob=$_
        $result=FormatADObjectJob -adObjectJob $ADObjectJob -randomStamp $randomStamp -timeStamp $timeStamp
        $result|ForEach-Object {
            $ADObjectItem=$_
            $class="AD"+$ADObjectItem.objectClass #Initial AD
            $dataMatrix[$class]+=$ADObjectItem
        }
     }
 
}
#.............................................................................................#

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

Get-Job|Remove-Job -Force -ErrorAction SilentlyContinue |Out-Null

#Main Body.........................................................................
$captureJob=@{}
$randomStamp=(Get-Random).Tostring()
$timeStamp=(Get-Date).Tostring()
$jobThrottleLimit=30 #Based on heuristics 
$ADCSVFolder="V:\tmp\ADCSVload"


$createSQLTables=$false  #Switch to create SQL tables
$captureData=$true       #Switch to capture data from computer list, needed to create tables
$loadData=$true          #Switch to load existing CSV data from folder
$deleteAfterLoad=$true
$archiveFolderName=($ADCSVFolder+"\Archive")
$classList=("ADComputer","ADUser") #AD needed since user and computer are SQL keywords

#Capture new data............................................................................................................
if ($captureData) {
    Get-Job|Remove-Job -Force #Clean jobs
    "Gathering data..." #Console message

    #Create AD data capture jobs................................................................................................
    $jobMatrix=@{}
    CreateADObjectJobFromDCList   -classList $classList -randomStamp $randomStamp -timeStamp $timeStamp -jobMatrix $jobMatrix


    #Collect AD data jobs results...............................................................................................
    $jobList=Get-Job|sort State

    "Saving Data..." #Console message
    $jobList.Count

    #Initialize data matrix......................................................................................................
    $dataMatrix=@{} 

    #By class
    $classlist|ForEach-Object {
        $dataMatrix[$_]=@()
    }

    $jobsTimeOut=600 #In seconds, 300=5 minutes, 600=10 minutes, etc.
    #Wait for jobs to finish
    Get-Job|Wait-Job -Timeout  $jobsTimeOut |Out-Null
    $jobList=Get-Job -State Completed


    CollectDataFromJobListByClass  -jobList $jobList  -randomStamp $randomStamp -timeStamp $timeStamp -dataMAtrix $dataMatrix 


    "Formatting  Data..." #Console message
    #Create SQL tables from data.................................................................................................
    $tablesMatrix=@{} 
    $classlist|ForEach-Object {
        $tablesMatrix[$_]=@()
    }

    $dataMatrix.Keys|ForEach-Object {
        $class=$_
        $tablesMatrix[$class]=($dataMatrix[$class]|Out-SQLDataTable)
    }

    "Converting to CSV SQL friendly  Data..." #Console message
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
        $outputFile=$ADCSVFolder+"\"+$class+"-"+$randomStamp+".CSV"
        $dataCSV[$class]|Set-Content $outputFile -ErrorAction SilentlyContinue

      }
}

#Create SQL Tables............................................................................................................

# Run config_settings.ps1 or set manually
# $colMaxLength=600
# $SQLServer="sql-8402\SQLExpressPS"
# $SQLDatabase="objectsAD"

if ($createSQLTables -eq $true) {
      $dataCSV.Keys|ForEach-Object {
      $class=$_
      Create-SQLClassTable -class $class -dataCSV $dataCSV[$class] -csvDelimiter $csvDelimiter -SQLServer $SQLServer -SQLDatabase $SQLDatabase -colMaxLength $colMaxLength
      }

}

#Load data from existing CSV into SQL database............................................................................................

if ($loadData)
    {
    $csvFiles=get-childitem  $ADCSVFolder |where {$_.Name -like "*-*.CSV"}

    $connectionString = "Data Source="+$SQLServer+"; Integrated Security=True;Initial Catalog="+$SQLDatabase+";"
    $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $connectionString

    $preSQLdata=@{}
    $SQLdata=@{}

    $csvFiles|ForEach-Object{
        $class=$_.Name.Split("-")[0]
        
        $preSQLdata[$class]=$_|Get-Content|ConvertFrom-Csv -Delimiter $csvDelimiter
        $SQLdata[$class]=($preSQLdata[$class]|Out-SQLDataTable)

        #Save to SQL
        $bulkCopy.DestinationTableName = $class
        $errCode=$bulkCopy.WriteToServer($SQLdata[$class]) 
        #$errCode

    }
    
if ($deleteAfterLoad)
    {
        $archiveFolder=New-Item $archiveFolderName -ItemType Directory -Force -ErrorAction SilentlyContinue
        if ($archiveFolder.Exists) {
                $csvFiles|Move-Item -Destination $archiveFolderName -Force -ErrorAction SilentlyContinue
        #       $csvFiles|Remove-Item -Force -ErrorAction SilentlyContinue
       }
    }

}





