# Running AD identity requirements
# AD: domain device  tree inspection permissions
# SQL access requirement: database write permissions for AD identity
# Network: connectivity to remote hosts

# AD scanning
$colMaxLength=600
$SQLServer="sql-8402\SQLExpressPS"
$SQLDatabase="objectsAD"

# Junos Pulse VPN Network scanning
# IP range configuration for VPN range
$base="172.26.80."
$range=1..255

