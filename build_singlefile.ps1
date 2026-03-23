param(
    [string]$PythonExe = "python",
    [string]$AppName = "Plunger"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $root

try {
    Write-Host "Ensuring PyInstaller is installed..."
    & $PythonExe -m pip install pyinstaller

    Write-Host "Building single-file executable..."
    & $PythonExe -m PyInstaller `
        --noconfirm `
        --clean `
        --onefile `
        --windowed `
        --name $AppName `
        --hidden-import proxy_ui `
        --hidden-import resilient_proxy `
        --collect-submodules aiohttp `
        run.pyw

    $output = Join-Path $root ("dist\" + $AppName + ".exe")
    Write-Host ""
    Write-Host "Build complete:"
    Write-Host $output
}
finally {
    Pop-Location
}
