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

# Run config_settings.ps1
# Or set manually
# IP range configuration for VPN range
# $base="172.26.80."
# $range=1..255

$online=@()
$sampleList=@()

$range|ForEach-Object {
    $endpoint=$base+$_
    $sampleList+=$endpoint
}



#Sample Computers to find online computers..............................................................................................
Get-Job|Remove-Job -Force 

$sampleJobs=@()
$sampleJobsTimeOut=30 #Seconds

$sampleList | pingJob -throttleLimit $jobThrottleLimit
Get-Job|Wait-Job -Timeout $sampleJobsTimeOut |Out-Null
#Select Online computers 
$online =(Get-Job -State Completed|Receive-job)|Where {$_.ResponseTime -ne $null}
$testlist=$online|ForEach-Object {$_.Address}

$info=@{}

$secStgring=Read-Host -assecurestring | ConvertFrom-SecureString
$password=$secStgring|ConvertTo-SecureString


$testlist


$testlist|ForEach-Object{
    $username="$_"+"\ltadmin"
    $credential=New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $username, $password
    $info[$_]=gwmi -class Win32_ComputerSystem -ComputerName $_ -Credential $credential  -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -InformationAction SilentlyContinue
    Invoke-WmiMethod -Class Win32_Process -Name Create -ArgumentList "ipconfig /registerdns"  -ComputerName $_ -ErrorAction SilentlyContinue #-Credential $credential
}


$info.Keys |sort|ForEach-Object {$_+"   Live info: "+$info[$_].Name+" DNS info: " +(Resolve-DnsName -Name $_).NameHost}