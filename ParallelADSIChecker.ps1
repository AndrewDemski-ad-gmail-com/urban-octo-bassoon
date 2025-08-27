#requires -Version 5.1

<#
.SYNOPSIS
    Parallel Active Directory failed logon attempt checker using ADSI (no RSAT required)

.DESCRIPTION
    Queries multiple domain controllers in parallel to get accurate failed logon counts.
    Uses either background jobs or runspaces for parallel execution.
    Compatible with PowerShell 5.1+

.NOTES
    Author: Generated for testing parallel ADSI operations
    Version: 1.0
    Requires: Domain-joined machine or credentials for AD access
    
    IMPORTANT: badPwdCount is NOT replicated between DCs - each DC maintains its own count.
    This script queries all DCs and aggregates the results for accurate totals.
#>

#region Helper Functions

function Test-ADSIConnection {
    <#
    .SYNOPSIS
        Tests basic ADSI connectivity to domain
    #>
    [CmdletBinding()]
    param()
    
    try {
        Write-Host "Testing ADSI connection..." -ForegroundColor Yellow
        $rootDSE = [ADSI]"LDAP://RootDSE"
        $domainDN = $rootDSE.defaultNamingContext
        $domain = [System.DirectoryServices.ActiveDirectory.Domain]::GetCurrentDomain()
        
        Write-Host "✓ Domain DN: $domainDN" -ForegroundColor Green
        Write-Host "✓ Domain: $($domain.Name)" -ForegroundColor Green
        Write-Host "✓ Domain Controllers found: $($domain.DomainControllers.Count)" -ForegroundColor Green
        
        return @{
            Success = $true
            DomainDN = $domainDN
            DomainName = $domain.Name
            DCCount = $domain.DomainControllers.Count
        }
    }
    catch {
        Write-Host "✗ ADSI Connection failed: $($_.Exception.Message)" -ForegroundColor Red
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

function Show-FailedLogonSummary {
    <#
    .SYNOPSIS
        Displays formatted summary of failed logon results
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [array]$Results,
        [string]$Username
    )
    
    $successful = $Results | Where-Object Success
    $failed = $Results | Where-Object { -not $_.Success }
    
    Write-Host "`n=== Failed Logon Summary for '$Username' ===" -ForegroundColor Cyan
    Write-Host "Successfully queried DCs: $($successful.Count)" -ForegroundColor Green
    Write-Host "Failed DC queries: $($failed.Count)" -ForegroundColor Red
    
    if ($successful) {
        $totalFailures = ($successful.BadPasswordCount | Measure-Object -Sum).Sum
        $maxFailures = $successful | Sort-Object BadPasswordCount -Descending | Select-Object -First 1
        $latestAttempt = $successful | Where-Object BadPasswordTime | Sort-Object BadPasswordTime -Descending | Select-Object -First 1
        $lockedDCs = $successful | Where-Object IsLocked
        $avgQueryTime = [Math]::Round(($successful.QueryDurationMs | Measure-Object -Average).Average, 2)
        
        Write-Host "`nTotal failed logons across all DCs: $totalFailures" -ForegroundColor Yellow
        if ($maxFailures) {
            Write-Host "Highest count on single DC: $($maxFailures.BadPasswordCount) ($($maxFailures.DomainController))"
        }
        
        if ($latestAttempt.BadPasswordTime) {
            Write-Host "Latest failed attempt: $($latestAttempt.BadPasswordTime) on $($latestAttempt.DomainController)"
        }
        
        if ($lockedDCs) {
            Write-Host "Account locked on: $($lockedDCs.DomainController -join ', ')" -ForegroundColor Red
        } else {
            Write-Host "Account status: Not locked" -ForegroundColor Green
        }
        
        Write-Host "Average query time: $avgQueryTime ms"
        
        Write-Host "`n=== Detailed Results ==="
        $successful | Sort-Object BadPasswordCount -Descending | 
            Format-Table DomainController, BadPasswordCount, BadPasswordTime, IsLocked, QueryDurationMs -AutoSize
    }
    
    if ($failed) {
        Write-Host "`n=== Failed Queries ===" -ForegroundColor Red
        $failed | Format-Table DomainController, Error -AutoSize
    }
    
    return @{
        TotalFailures = if ($successful) { ($successful.BadPasswordCount | Measure-Object -Sum).Sum } else { 0 }
        SuccessfulQueries = $successful.Count
        FailedQueries = $failed.Count
        IsLocked = ($successful | Where-Object IsLocked).Count -gt 0
    }
}

function Test-UserExists {
    <#
    .SYNOPSIS
        Quickly test if a user exists in AD before running parallel queries
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Username,
        [string]$DomainDN
    )
    
    try {
        if (-not $DomainDN) {
            $DomainDN = ([ADSI]"LDAP://RootDSE").defaultNamingContext
        }
        
        $searcher = [ADSISearcher]"(&(objectClass=user)(sAMAccountName=$Username))"
        $searcher.SearchRoot = [ADSI]"LDAP://$DomainDN"
        $searcher.PageSize = 1
        
        $result = $searcher.FindOne()
        if ($result) {
            $user = $result.GetDirectoryEntry()
            Write-Host "✓ User '$Username' found: $($user.distinguishedName)" -ForegroundColor Green
            return $true
        } else {
            Write-Host "✗ User '$Username' not found in domain" -ForegroundColor Red
            return $false
        }
    }
    catch {
        Write-Host "✗ Error checking user: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
    finally {
        if ($searcher) { $searcher.Dispose() }
    }
}

#endregion

#region Main Functions

function Get-UserFailedLogonsJobs {
    <#
    .SYNOPSIS
        Get failed logon attempts using PowerShell background jobs
    
    .DESCRIPTION
        Queries all domain controllers in parallel using background jobs to get
        accurate failed logon counts. Uses synchronized hashtable for thread safety.
    
    .PARAMETER Username
        sAMAccountName of the user to check
        
    .PARAMETER DomainDN
        Domain distinguished name (auto-detected if not specified)
        
    .PARAMETER TimeoutSeconds
        Timeout for individual DC queries (default: 30)
        
    .PARAMETER MaxConcurrentJobs
        Maximum number of parallel jobs (default: 10)
    
    .EXAMPLE
        Get-UserFailedLogonsJobs -Username "jdoe" -TimeoutSeconds 20
        
    .EXAMPLE
        $results = Get-UserFailedLogonsJobs -Username "testuser" -MaxConcurrentJobs 5
        Show-FailedLogonSummary -Results $results -Username "testuser"
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string]$Username,
        
        [string]$DomainDN,
        
        [ValidateRange(5, 120)]
        [int]$TimeoutSeconds = 30,
        
        [ValidateRange(1, 20)]
        [int]$MaxConcurrentJobs = 10
    )
    
    begin {
        Write-Verbose "Starting parallel job-based query for user: $Username"
        $startTime = Get-Date
    }
    
    process {
        try {
            # Auto-detect domain DN if not provided
            if (-not $DomainDN) {
                $DomainDN = ([ADSI]"LDAP://RootDSE").defaultNamingContext
                Write-Verbose "Auto-detected Domain DN: $DomainDN"
            }
            
            $domain = [System.DirectoryServices.ActiveDirectory.Domain]::GetCurrentDomain()
            $dcs = $domain.DomainControllers | Select-Object -First $MaxConcurrentJobs
            
            Write-Verbose "Found $($dcs.Count) domain controllers to query"
            Write-Progress -Activity "Querying Domain Controllers" -Status "Starting jobs..." -PercentComplete 0
            
            # Thread-safe synchronized hashtable for results
            $syncResults = [hashtable]::Synchronized(@{})
            
            $scriptBlock = {
                param($DCName, $User, $DomainDN, $SyncHash, $JobId)
                
                $result = [PSCustomObject]@{
                    JobId = $JobId
                    DomainController = $DCName
                    BadPasswordCount = 0
                    BadPasswordTime = $null
                    LockoutTime = $null
                    IsLocked = $false
                    Success = $false
                    Error = $null
                    QueryDurationMs = 0
                }
                
                $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
                
                try {
                    # Add connection timeout
                    $searcher = New-Object System.DirectoryServices.DirectorySearcher
                    $searcher.SearchRoot = New-Object System.DirectoryServices.DirectoryEntry("LDAP://$DCName/$DomainDN")
                    $searcher.Filter = "(&(objectClass=user)(sAMAccountName=$User))"
                    $searcher.PropertiesToLoad.AddRange(@("badPwdCount", "badPasswordTime", "lockoutTime", "sAMAccountName"))
                    $searcher.PageSize = 1000
                    $searcher.ServerTimeLimit = [TimeSpan]::FromSeconds(15)
                    
                    $searchResult = $searcher.FindOne()
                    if ($searchResult) {
                        $props = $searchResult.Properties
                        
                        $result.BadPasswordCount = if ($props["badpwdcount"]) { [int]$props["badpwdcount"][0] } else { 0 }
                        $result.BadPasswordTime = if ($props["badpasswordtime"] -and $props["badpasswordtime"][0] -gt 0) { 
                            [DateTime]::FromFileTime([long]$props["badpasswordtime"][0]) 
                        } else { $null }
                        $result.LockoutTime = if ($props["lockouttime"] -and $props["lockouttime"][0] -gt 0) { 
                            [DateTime]::FromFileTime([long]$props["lockouttime"][0]) 
                        } else { $null }
                        $result.IsLocked = $props["lockouttime"] -and ([long]$props["lockouttime"][0] -gt 0)
                        $result.Success = $true
                    }
                    else {
                        $result.Error = "User not found on $DCName"
                    }
                }
                catch {
                    $result.Error = "DC: $DCName - $($_.Exception.Message)"
                }
                finally {
                    $result.QueryDurationMs = $stopwatch.ElapsedMilliseconds
                    $stopwatch.Stop()
                    if ($searcher) { 
                        $searcher.Dispose() 
                    }
                }
                
                # Store result in synchronized hashtable
                $SyncHash[$JobId] = $result
                return $result
            }
            
            # Start jobs in batches to avoid overwhelming
            $jobs = @()
            $jobId = 0
            
            foreach ($dc in $dcs) {
                Write-Verbose "Starting job for DC: $($dc.Name)"
                $jobs += Start-Job -ScriptBlock $scriptBlock -ArgumentList @($dc.Name, $Username, $DomainDN, $syncResults, $jobId++)
                
                # Throttle job creation
                if ($jobs.Count -ge $MaxConcurrentJobs) {
                    Write-Progress -Activity "Querying Domain Controllers" -Status "Waiting for batch completion..." -PercentComplete 25
                    $jobs | Wait-Job -Timeout $TimeoutSeconds | Out-Null
                }
            }
            
            Write-Progress -Activity "Querying Domain Controllers" -Status "Waiting for all jobs..." -PercentComplete 50
            
            # Wait for all jobs with timeout
            $completedJobs = $jobs | Wait-Job -Timeout $TimeoutSeconds
            $timedOutJobs = $jobs | Where-Object { $_.State -eq 'Running' }
            
            if ($timedOutJobs) {
                Write-Warning "Stopping $($timedOutJobs.Count) timed-out jobs"
                $timedOutJobs | Stop-Job
            }
            
            Write-Progress -Activity "Querying Domain Controllers" -Status "Collecting results..." -PercentComplete 75
            
            # Collect results from completed jobs and synchronized hashtable
            $allResults = @()
            foreach ($job in $completedJobs) {
                try {
                    $jobResult = Receive-Job -Job $job -ErrorAction SilentlyContinue
                    if ($jobResult) {
                        $allResults += $jobResult
                    }
                }
                catch {
                    Write-Warning "Failed to receive job result: $($_.Exception.Message)"
                }
            }
            
            # Add any results that made it to the synchronized hashtable
            foreach ($key in $syncResults.Keys) {
                $syncResult = $syncResults[$key]
                if ($syncResult -and ($allResults | Where-Object { $_.JobId -eq $syncResult.JobId }).Count -eq 0) {
                    $allResults += $syncResult
                }
            }
            
            Write-Progress -Activity "Querying Domain Controllers" -Completed
            
            $totalTime = ((Get-Date) - $startTime).TotalMilliseconds
            Write-Verbose "Query completed in $([Math]::Round($totalTime, 2)) ms"
            
            return $allResults
        }
        catch {
            Write-Error "Failed to query domain controllers: $($_.Exception.Message)"
            return @()
        }
        finally {
            # Cleanup all jobs
            if ($jobs) {
                Write-Verbose "Cleaning up $($jobs.Count) background jobs"
                $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

function Get-UserFailedLogonsRunspace {
    <#
    .SYNOPSIS
        Get failed logon attempts using PowerShell runspaces (faster than jobs)
    
    .DESCRIPTION
        Queries all domain controllers in parallel using runspaces for maximum performance.
        Generally faster than background jobs but uses more memory.
    
    .PARAMETER Username
        sAMAccountName of the user to check
        
    .PARAMETER DomainDN
        Domain distinguished name (auto-detected if not specified)
        
    .PARAMETER TimeoutSeconds
        Timeout for individual DC queries (default: 30)
        
    .PARAMETER MaxRunspaces
        Maximum number of parallel runspaces (default: 10)
    
    .EXAMPLE
        Get-UserFailedLogonsRunspace -Username "jdoe" -TimeoutSeconds 20
        
    .EXAMPLE
        $results = Get-UserFailedLogonsRunspace -Username "testuser" -MaxRunspaces 5
        Show-FailedLogonSummary -Results $results -Username "testuser"
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string]$Username,
        
        [string]$DomainDN,
        
        [ValidateRange(5, 120)]
        [int]$TimeoutSeconds = 30,
        
        [ValidateRange(1, 20)]
        [int]$MaxRunspaces = 10
    )
    
    begin {
        Write-Verbose "Starting parallel runspace-based query for user: $Username"
        $startTime = Get-Date
    }
    
    process {
        try {
            if (-not $DomainDN) {
                $DomainDN = ([ADSI]"LDAP://RootDSE").defaultNamingContext
                Write-Verbose "Auto-detected Domain DN: $DomainDN"
            }
            
            $domain = [System.DirectoryServices.ActiveDirectory.Domain]::GetCurrentDomain()
            $dcs = $domain.DomainControllers
            
            Write-Verbose "Found $($dcs.Count) domain controllers to query"
            
            # Create initial session state with required assemblies
            $initialSessionState = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
            $initialSessionState.ImportPSModule(@("Microsoft.PowerShell.Management", "Microsoft.PowerShell.Utility"))
            
            # Create runspace pool
            $runspacePool = [runspacefactory]::CreateRunspacePool(1, [Math]::Min($MaxRunspaces, $dcs.Count), $initialSessionState, $Host)
            $runspacePool.ApartmentState = [System.Threading.ApartmentState]::MTA
            $runspacePool.Open()
            
            Write-Verbose "Created runspace pool with max $([Math]::Min($MaxRunspaces, $dcs.Count)) runspaces"
            
            $scriptBlock = {
                param($DCName, $User, $DomainDN, $TimeoutMs)
                
                $result = [PSCustomObject]@{
                    DomainController = $DCName
                    BadPasswordCount = 0
                    BadPasswordTime = $null
                    LockoutTime = $null
                    IsLocked = $false
                    Success = $false
                    Error = $null
                    QueryDurationMs = 0
                }
                
                $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
                $searcher = $null
                
                try {
                    # Create searcher with timeout
                    $searcher = New-Object System.DirectoryServices.DirectorySearcher
                    $searcher.SearchRoot = New-Object System.DirectoryServices.DirectoryEntry("LDAP://$DCName/$DomainDN")
                    $searcher.Filter = "(&(objectClass=user)(sAMAccountName=$User))"
                    $searcher.PropertiesToLoad.AddRange(@("badPwdCount", "badPasswordTime", "lockoutTime"))
                    $searcher.ServerTimeLimit = [TimeSpan]::FromMilliseconds($TimeoutMs)
                    $searcher.ClientTimeout = [TimeSpan]::FromMilliseconds($TimeoutMs)
                    $searcher.PageSize = 1000
                    
                    $searchResult = $searcher.FindOne()
                    if ($searchResult) {
                        $props = $searchResult.Properties
                        
                        $result.BadPasswordCount = if ($props["badpwdcount"]) { [int]$props["badpwdcount"][0] } else { 0 }
                        $result.BadPasswordTime = if ($props["badpasswordtime"] -and $props["badpasswordtime"][0] -gt 0) { 
                            [DateTime]::FromFileTime([long]$props["badpasswordtime"][0]) 
                        } else { $null }
                        $result.LockoutTime = if ($props["lockouttime"] -and $props["lockouttime"][0] -gt 0) { 
                            [DateTime]::FromFileTime([long]$props["lockouttime"][0]) 
                        } else { $null }
                        $result.IsLocked = $props["lockouttime"] -and ([long]$props["lockouttime"][0] -gt 0)
                        $result.Success = $true
                    } else {
                        $result.Error = "User '$User' not found on $DCName"
                    }
                }
                catch [System.DirectoryServices.DirectoryServiceCOMException] {
                    $result.Error = "LDAP Error on $DCName`: $($_.Exception.Message)"
                }
                catch [System.TimeoutException] {
                    $result.Error = "Timeout querying $DCName"
                }
                catch {
                    $result.Error = "Error querying $DCName`: $($_.Exception.Message)"
                }
                finally {
                    $result.QueryDurationMs = $stopwatch.ElapsedMilliseconds
                    $stopwatch.Stop()
                    if ($searcher) { 
                        $searcher.Dispose() 
                    }
                }
                
                return $result
            }
            
            # Start runspaces
            $runspaces = @()
            $timeoutMs = $TimeoutSeconds * 1000
            
            Write-Progress -Activity "Querying Domain Controllers" -Status "Starting queries..." -PercentComplete 0
            
            foreach ($dc in $dcs) {
                $powerShell = [powershell]::Create()
                $powerShell.RunspacePool = $runspacePool
                [void]$powerShell.AddScript($scriptBlock).AddArgument($dc.Name).AddArgument($Username).AddArgument($DomainDN).AddArgument($timeoutMs)
                
                $runspaces += [PSCustomObject]@{
                    PowerShell = $powerShell
                    AsyncResult = $powerShell.BeginInvoke()
                    DCName = $dc.Name
                }
                
                Write-Verbose "Started runspace for DC: $($dc.Name)"
            }
            
            # Wait for completion with progress
            $results = @()
            $completed = 0
            $startTime = Get-Date
            
            while ($completed -lt $runspaces.Count -and ((Get-Date) - $startTime).TotalSeconds -lt $TimeoutSeconds) {
                foreach ($runspace in $runspaces | Where-Object { $_.AsyncResult -and -not $_.AsyncResult.IsCompleted }) {
                    if ($runspace.AsyncResult.IsCompleted) {
                        try {
                            $result = $runspace.PowerShell.EndInvoke($runspace.AsyncResult)
                            $results += $result
                            $completed++
                            
                            Write-Verbose "Completed query for DC: $($runspace.DCName)"
                            
                            $percentComplete = [Math]::Round(($completed / $runspaces.Count) * 100)
                            Write-Progress -Activity "Querying Domain Controllers" -Status "Completed: $completed/$($runspaces.Count)" -PercentComplete $percentComplete
                        }
                        catch {
                            Write-Warning "Failed to get result from $($runspace.DCName): $($_.Exception.Message)"
                            $completed++
                        }
                        finally {
                            $runspace.AsyncResult = $null
                        }
                    }
                }
                Start-Sleep -Milliseconds 100
            }
            
            # Handle any remaining incomplete operations
            $incomplete = $runspaces | Where-Object { $_.AsyncResult }
            if ($incomplete) {
                Write-Warning "$($incomplete.Count) operations did not complete within timeout"
            }
            
            Write-Progress -Activity "Querying Domain Controllers" -Completed
            
            $totalTime = ((Get-Date) - $startTime).TotalMilliseconds
            Write-Verbose "Query completed in $([Math]::Round($totalTime, 2)) ms"
            
            return $results
        }
        catch {
            Write-Error "Runspace operation failed: $($_.Exception.Message)"
            return @()
        }
        finally {
            # Comprehensive cleanup
            if ($runspaces) {
                Write-Verbose "Cleaning up $($runspaces.Count) runspaces"
                foreach ($runspace in $runspaces) {
                    if ($runspace.PowerShell) {
                        $runspace.PowerShell.Dispose()
                    }
                }
            }
            if ($runspacePool) {
                $runspacePool.Close()
                $runspacePool.Dispose()
            }
        }
    }
}

#endregion

#region Usage Examples and Testing Functions

function Invoke-FailedLogonTest {
    <#
    .SYNOPSIS
        Test function to validate both parallel methods
    
    .EXAMPLE
        Invoke-FailedLogonTest -Username "testuser"
        
    .EXAMPLE
        Invoke-FailedLogonTest -Username "testuser" -ComparePerformance
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Username,
        [switch]$ComparePerformance
    )
    
    Write-Host "=== Testing Parallel ADSI Failed Logon Checker ===" -ForegroundColor Magenta
    
    # Test connectivity first
    $connection = Test-ADSIConnection
    if (-not $connection.Success) {
        Write-Error "ADSI connection test failed. Cannot proceed."
        return
    }
    
    # Test if user exists
    if (-not (Test-UserExists -Username $Username)) {
        Write-Error "User '$Username' not found. Cannot proceed."
        return
    }
    
    if ($ComparePerformance) {
        Write-Host "`n--- Testing Background Jobs Method ---" -ForegroundColor Yellow
        $jobsStart = Get-Date
        $jobsResults = Get-UserFailedLogonsJobs -Username $Username -Verbose
        $jobsTime = ((Get-Date) - $jobsStart).TotalMilliseconds
        
        Write-Host "`n--- Testing Runspaces Method ---" -ForegroundColor Yellow
        $runspacesStart = Get-Date
        $runspacesResults = Get-UserFailedLogonsRunspace -Username $Username -Verbose
        $runspacesTime = ((Get-Date) - $runspacesStart).TotalMilliseconds
        
        Write-Host "`n=== Performance Comparison ===" -ForegroundColor Cyan
        Write-Host "Jobs method: $([Math]::Round($jobsTime, 2)) ms"
        Write-Host "Runspaces method: $([Math]::Round($runspacesTime, 2)) ms"
        Write-Host "Winner: " -NoNewline
        if ($runspacesTime -lt $jobsTime) {
            Write-Host "Runspaces (faster by $([Math]::Round($jobsTime - $runspacesTime, 2)) ms)" -ForegroundColor Green
        } else {
            Write-Host "Jobs (faster by $([Math]::Round($runspacesTime - $jobsTime, 2)) ms)" -ForegroundColor Green
        }
        
        Show-FailedLogonSummary -Results $runspacesResults -Username $Username
    } else {
        # Just run runspaces method (typically faster)
        Write-Host "`n--- Running Runspaces Method ---" -ForegroundColor Yellow
        $results = Get-UserFailedLogonsRunspace -Username $Username -Verbose
        Show-FailedLogonSummary -Results $results -Username $Username
    }
}

# Example usage scenarios
<#
# Basic usage:
$results = Get-UserFailedLogonsRunspace -Username "jdoe"
Show-FailedLogonSummary -Results $results -Username "jdoe"

# With custom timeout:
$results = Get-UserFailedLogonsJobs -Username "testuser" -TimeoutSeconds 15

# Test both methods and compare performance:
Invoke-FailedLogonTest -Username "testuser" -ComparePerformance

# Quick connectivity test:
Test-ADSIConnection

# Check if user exists before running expensive parallel queries:
Test-UserExists -Username "someuser"
#>

#endregion