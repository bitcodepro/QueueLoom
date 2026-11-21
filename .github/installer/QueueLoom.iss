; Built from the same staging directory as the portable ZIP.
[Setup]
AppId=QueueLoom
AppName=QueueLoom
AppVersion={#ProductVersion}
AppPublisher=bitcodepro
AppPublisherURL=https://github.com/bitcodepro/QueueLoom
DefaultDirName={localappdata}\Programs\QueueLoom
UsePreviousAppDir=yes
DisableDirPage=yes
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
UninstallDisplayIcon={app}\QueueLoom.exe
SetupIconFile=..\..\src\QueueLoom.App\Assets\queueloom.ico
OutputDir={#PackageOutput}
OutputBaseFilename=QueueLoom-{#ProductVersion}-win-x64-setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
UsePreviousTasks=yes
SetupMutex=QueueLoomInstaller

[Tasks]
Name: desktopicon; Description: "Create a desktop shortcut"; Flags: unchecked

[Files]
; Put temporary files first so solid extraction does not traverse the launcher.
Source: "{#PackageInput}\QueueLoom.bootstrap.json"; Flags: dontcopy noencryption
Source: "{#PackageInput}\QueueLoom.bootstrap.zip"; Flags: dontcopy noencryption
; A manual upgrade keeps the installed entry and fallback bootstrap byte-for-byte.
Source: "{#PackageInput}\QueueLoom.exe"; DestDir: "{app}"; Check: InstallBootstrap
Source: "{#PackageInput}\QueueLoom.bootstrap.json"; DestDir: "{app}"; Check: InstallBootstrap
Source: "{#PackageInput}\QueueLoom.bootstrap.zip"; DestDir: "{app}"; Check: InstallBootstrap
Source: "owner-marker.txt"; DestDir: "{app}"; DestName: ".queueloom-installer"; Flags: ignoreversion uninsneveruninstall; Check: InstallBootstrap
Source: "{#PackageInput}\README.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PackageInput}\LICENSE"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PackageInput}\THIRD-PARTY-NOTICES.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PackageInput}\docs\stable-launcher.md"; DestDir: "{app}\docs"; Flags: ignoreversion

[Icons]
Name: "{userprograms}\QueueLoom"; Filename: "{app}\QueueLoom.exe"; WorkingDir: "{app}"
Name: "{userdesktop}\QueueLoom"; Filename: "{app}\QueueLoom.exe"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\QueueLoom.exe"; Description: "Launch QueueLoom"; WorkingDir: "{app}"; Flags: postinstall nowait skipifsilent; Check: ActivationSucceeded

[Code]
const
  InstallKey = 'Software\Microsoft\Windows\CurrentVersion\Uninstall\QueueLoom_is1';
  OwnerMarker = '\.queueloom-installer';

var
  Existing: Boolean;
  Reinstall: Boolean;
  ActivationFailed: Boolean;

function InstallBootstrap: Boolean;
begin
  Result := not Existing;
end;

function ActivationSucceeded: Boolean;
begin
  Result := not ActivationFailed;
end;

function DirectoryIsEmpty(Path: String): Boolean;
var
  Entry: TFindRec;
begin
  Result := True;
  if FindFirst(Path + '\*', Entry) then
  try
    repeat
      if (Entry.Name <> '.') and (Entry.Name <> '..') then
      begin
        Result := False;
        Exit;
      end;
    until not FindNext(Entry);
  finally
    FindClose(Entry);
  end;
end;

function RunPackageCommand(AppDir, PackageDir, Command: String): Boolean;
var
  ExitCode: Integer;
  Parameters: String;
begin
  Parameters := Command;
  if PackageDir <> '' then Parameters := Parameters + ' "' + PackageDir + '"';
  Result := Exec(AppDir + '\QueueLoom.exe',
    Parameters, AppDir, SW_HIDE,
    ewWaitUntilTerminated, ExitCode) and (ExitCode = 0);
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
var
  AppDir, RegisteredDir: String;
  Marker: AnsiString;
begin
  Result := '';
  AppDir := ExpandConstant('{app}');
  Existing := RegQueryStringValue(HKCU, InstallKey, 'InstallLocation', RegisteredDir);
  Reinstall := False;
  if Existing then
  begin
    if CompareText(RemoveBackslash(RegisteredDir), RemoveBackslash(AppDir)) <> 0 then
    begin
      Result := 'Uninstall QueueLoom before changing the installation directory.';
      Exit;
    end;
    if not FileExists(AppDir + '\QueueLoom.exe') then
    begin
      Result := 'The installed QueueLoom launcher is missing. Uninstall QueueLoom before reinstalling it.';
      Exit;
    end;
    ExtractTemporaryFile('QueueLoom.bootstrap.json');
    ExtractTemporaryFile('QueueLoom.bootstrap.zip');
    if not RunPackageCommand(AppDir, ExpandConstant('{tmp}'), '--installer-check-package') then
      Result := 'QueueLoom could not validate this version. Close QueueLoom and its MCP clients, finish any pending update by launching the app, and retry with the latest installer.';
  end
  else if DirExists(AppDir) and not DirectoryIsEmpty(AppDir) then
  begin
    Reinstall := LoadStringFromFile(AppDir + OwnerMarker, Marker) and
      (Marker = 'QueueLoom Installer v1') and
      not FileExists(AppDir + '\QueueLoom.exe') and
      not FileExists(AppDir + '\QueueLoom.bootstrap.json') and
      not FileExists(AppDir + '\QueueLoom.bootstrap.zip');
    if not Reinstall then
      Result := 'The installation directory contains existing files. Move that portable installation to a different folder before running Setup.';
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  AppDir: String;
begin
  if CurStep = ssPostInstall then
  begin
    AppDir := ExpandConstant('{app}');
    // The preflight above never publishes an activation record. Only now,
    // after Setup copied the ordinary files and registered uninstall, stage the candidate.
    if Existing then
      ActivationFailed := not RunPackageCommand(AppDir, ExpandConstant('{tmp}'), '--installer-stage-package')
    else
    begin
      ActivationFailed := not RunPackageCommand(AppDir, '', '--installer-record-bootstrap');
      if Reinstall and not ActivationFailed then
        ActivationFailed := not RunPackageCommand(AppDir, AppDir, '--installer-stage-package');
    end;
    if ActivationFailed then
      SuppressibleMsgBox('Setup could not stage this version. Your existing application and data were retained. Open QueueLoom to finish any pending update, then run the latest Setup again.', mbError, MB_OK, IDOK);
  end;
end;

function GetCustomSetupExitCode: Integer;
begin
  if ActivationFailed then Result := 20 else Result := 0;
end;
