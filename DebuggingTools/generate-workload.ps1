param (
    [int]$start
)
Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$connectionString = "Data Source=(local);Integrated Security=SSPI;Initial Catalog=benchmark;Application Name=testapp"

$connection = new-object system.data.SqlClient.SQLConnection($connectionString)
$connection.Open()

$command = new-object system.data.sqlclient.sqlcommand("INSERT INTO benchmark VALUES(@p0)",$connection)
$command.Parameters.AddWithValue("@p0",0) | Out-Null

for($i = $start; $i -lt ($start + 100000); $i++) {
    $command.Parameters[0].Value = $i
    $command.ExecuteNonQuery() | Out-Null
    if($i % 1000 -eq 0) {$i}
}
$i

$connection.Close()
Get-Date -Format "yyyy-MM-dd HH:mm:ss"