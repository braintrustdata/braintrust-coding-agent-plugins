@echo off
REM Thin, fail-open Codex hook shim for the shared Braintrust daemon.
REM Invokes: bt daemon hook --source codex
setlocal EnableExtensions DisableDelayedExpansion

set "BT_HOOK_BIN="
for /f "delims=" %%B in ('where bt.exe 2^>nul') do if not defined BT_HOOK_BIN set "BT_HOOK_BIN=%%B"
if not defined BT_HOOK_BIN for /f "delims=" %%B in ('where bt.cmd 2^>nul') do if not defined BT_HOOK_BIN set "BT_HOOK_BIN=%%B"
if not defined BT_HOOK_BIN for /f "delims=" %%B in ('where bt 2^>nul') do if not defined BT_HOOK_BIN set "BT_HOOK_BIN=%%B"
if not defined BT_HOOK_BIN if exist "%USERPROFILE%\.local\bin\bt.exe" set "BT_HOOK_BIN=%USERPROFILE%\.local\bin\bt.exe"
if not defined BT_HOOK_BIN (
  echo trace-codex: bt CLI is unavailable; tracing disabled for this event.>&2
  exit /b 0
)

call "%BT_HOOK_BIN%" daemon hook --help >nul 2>&1
if errorlevel 1 (
  echo trace-codex: a daemon-capable bt CLI is unavailable; tracing disabled for this event.>&2
  exit /b 0
)

if not defined BRAINTRUST_DEFAULT_PROJECT if defined BRAINTRUST_PROJECT set "BRAINTRUST_DEFAULT_PROJECT=%BRAINTRUST_PROJECT%"

set "BT_PLUGIN_JSON=%~dp0..\.codex-plugin\plugin.json"
powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='SilentlyContinue';" ^
  "$a=@('daemon','hook','--source','codex');" ^
  "if(Test-Path $env:BT_PLUGIN_JSON){$v=(Get-Content -Raw $env:BT_PLUGIN_JSON|ConvertFrom-Json).version;if($v){$a+=@('--source-version',[string]$v)}};" ^
  "if($env:BRAINTRUST_FLUSH_ON_TURN_END -match '^(?i:1|true|yes|on)$'){$a+='--flush-on-turn-end'};" ^
  "if($env:CODEX_PARENT_SPAN_ID){$a+=@('--parent-span-id',$env:CODEX_PARENT_SPAN_ID)};" ^
  "if($env:CODEX_ROOT_SPAN_ID){$a+=@('--root-span-id',$env:CODEX_ROOT_SPAN_ID)};" ^
  "if($env:BRAINTRUST_ADDITIONAL_METADATA){$a+=@('--additional-metadata',$env:BRAINTRUST_ADDITIONAL_METADATA)};" ^
  "& $env:BT_HOOK_BIN @a;" ^
  "if($LASTEXITCODE -ne 0){[Console]::Error.WriteLine('trace-codex: bt daemon hook failed non-fatally')};exit 0"

exit /b 0
