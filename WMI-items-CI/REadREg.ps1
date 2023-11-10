# Helper Script to record Windows registry information
Get-PSDrive -Name RUSER |Remove-PSDrive -ErrorAction SilentlyContinue

$registryDrive=New-PSDrive -PSProvider Registry -Name RUSER -Root HKEY_USERS 

$userSIDs=get-item -path ($registryDrive.Name+":\*") 
$output=@()
$userSIDs|ForEach-Object {
    $item=Get-Item -Path ($registryDrive.NAme+":\"+$_.Name+"\Volatile Environment") -ErrorAction SilentlyContinue
    if ($item.Length -ge 1){
        $user=$item.GetValue("USERNAME")
        $appdata=$item.GetValue("APPDATA")
        $user+";" +$appdata
    }

}