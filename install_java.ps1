# Script pour installer Java 11 (OpenJDK) sur Windows
# Requis pour PySpark

Write-Host "Installation de Java 11 (OpenJDK) pour PySpark..." -ForegroundColor Green

$javaVersion = "11.0.22_7"
$javaDir = "jdk-11.0.22+7"
$downloadUrl = "https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.22%2B7/OpenJDK11U-jdk_x64_windows_hotspot_11.0.22_7.zip"
$zipFile = "$env:TEMP\openjdk11.zip"
$installDir = "C:\Java"

# Creer le repertoire d'installation
if (-not (Test-Path $installDir)) {
    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
}

# Telecharger Java
Write-Host "Telechargement de OpenJDK 11..." -ForegroundColor Yellow
Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile -UseBasicParsing

# Extraire
Write-Host "Extraction..." -ForegroundColor Yellow
Expand-Archive -Path $zipFile -DestinationPath $installDir -Force

# Configurer JAVA_HOME
$javaHome = "$installDir\$javaDir"
[Environment]::SetEnvironmentVariable("JAVA_HOME", $javaHome, "User")
$env:JAVA_HOME = $javaHome

# Ajouter au PATH
$currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($currentPath -notlike "*$javaHome\bin*") {
    $newPath = "$javaHome\bin;$currentPath"
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    $env:Path = "$javaHome\bin;$env:Path"
}

# Nettoyer
Remove-Item $zipFile -Force

Write-Host "Java 11 installe avec succes!" -ForegroundColor Green
Write-Host "JAVA_HOME: $javaHome" -ForegroundColor Cyan
Write-Host "Veuillez redemarrer votre terminal ou executer:" -ForegroundColor Yellow
Write-Host "   `$env:JAVA_HOME = '$javaHome'" -ForegroundColor White
Write-Host "   `$env:Path = '$javaHome\bin;' + `$env:Path" -ForegroundColor White

# Verifier l'installation
Write-Host "`nTest de l'installation..." -ForegroundColor Yellow
& "$javaHome\bin\java" -version
