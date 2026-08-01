param(
    [string]$RuntimeRoot = "$PSScriptRoot\..\.runtime\java17",
    [string]$DownloadPath = "$env:TEMP\novaretail-temurin17.zip"
)

$ErrorActionPreference = "Stop"
$metadataUri = "https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=x64&image_type=jdk&os=windows&vendor=eclipse"
$resolvedRuntimeRoot = [System.IO.Path]::GetFullPath($RuntimeRoot)
$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))

if (-not $resolvedRuntimeRoot.StartsWith($projectRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "RuntimeRoot must remain inside the project: $resolvedRuntimeRoot"
}

$existingJava = Get-ChildItem -LiteralPath $resolvedRuntimeRoot -Recurse -Filter java.exe -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match "\\bin\\java.exe$" } |
    Select-Object -First 1 -ExpandProperty FullName

if ($existingJava) {
    & $existingJava -version
    Write-Output "Java already installed: $existingJava"
    exit 0
}

$metadata = Invoke-RestMethod -Uri $metadataUri -TimeoutSec 30
$package = $metadata[0].binary.package

& curl.exe --fail --location --retry 3 --continue-at - --output $DownloadPath $package.link
if ($LASTEXITCODE -ne 0) {
    throw "JDK download failed with curl exit code $LASTEXITCODE"
}

$actual = (Get-FileHash -LiteralPath $DownloadPath -Algorithm SHA256).Hash.ToLowerInvariant()
$expected = $package.checksum.ToLowerInvariant()
if ($actual -ne $expected) {
    throw "JDK checksum mismatch. Expected $expected, got $actual"
}

New-Item -ItemType Directory -Force -Path $resolvedRuntimeRoot | Out-Null
Expand-Archive -LiteralPath $DownloadPath -DestinationPath $resolvedRuntimeRoot -Force
$javaExe = Get-ChildItem -LiteralPath $resolvedRuntimeRoot -Recurse -Filter java.exe |
    Where-Object { $_.FullName -match "\\bin\\java.exe$" } |
    Select-Object -First 1 -ExpandProperty FullName

if (-not $javaExe) {
    throw "java.exe was not found after extracting the verified archive"
}

Remove-Item -LiteralPath $DownloadPath -Force
& $javaExe -version
Write-Output "Installed checksum-verified Java: $javaExe"
