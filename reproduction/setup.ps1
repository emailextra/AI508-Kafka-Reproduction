param(
    [Parameter(Mandatory = $true)]
    [string]$Python
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$runtime = Join-Path $root ".runtime"
$upstream = Join-Path $root "upstream"
$commit = "19e9874c9414ceb1c036f856f1297a84e82ea4f3"
New-Item -ItemType Directory -Force $runtime | Out-Null

function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Program failed with exit code $LASTEXITCODE."
    }
}

if (!(Test-Path (Join-Path $upstream ".git"))) {
    Invoke-Checked git @("clone", "https://github.com/ccicconetti/kafka-hdd.git", $upstream)
}
Invoke-Checked git @("-C", $upstream, "checkout", "--detach", $commit)

if (!(Get-ChildItem $runtime -Directory -Filter "jdk-17.*")) {
    $zip = Join-Path $runtime "java.zip"
    $javaUrl = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.21%2B9/OpenJDK17U-jdk_x64_windows_hotspot_17.0.21_9.zip"
    Invoke-Checked curl.exe @("-fL", $javaUrl, "-o", $zip)
    Invoke-Checked tar.exe @("-xf", $zip, "-C", $runtime)
}

if (!(Test-Path (Join-Path $runtime "kafka_2.13-3.4.1/libs"))) {
    $archive = Join-Path $runtime "kafka.tgz"
    $checksum = Join-Path $runtime "kafka.sha512"
    $kafkaUrl = "https://archive.apache.org/dist/kafka/3.4.1/kafka_2.13-3.4.1.tgz"
    Invoke-Checked curl.exe @("-fL", $kafkaUrl, "-o", $archive)
    Invoke-Checked curl.exe @("-fL", "$kafkaUrl.sha512", "-o", $checksum)
    $expected = ([regex]::Match((Get-Content $checksum -Raw) -replace '\s', '', '[a-fA-F0-9]{128}')).Value
    if ((Get-FileHash $archive -Algorithm SHA512).Hash -ne $expected) {
        throw "Kafka archive failed the SHA-512 verification."
    }
    Invoke-Checked tar.exe @("-xzf", $archive, "-C", $runtime)
}

Invoke-Checked $Python @("-m", "pip", "install", "--target", (Join-Path $runtime "python"),
                        "confluent-kafka==2.16.1", "psutil==7.2.2")
Invoke-Checked $Python @("-m", "pip", "install", "--target", (Join-Path $runtime "analysis"),
                        "matplotlib==3.11.2", "python-docx==1.2.0")
